/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.kafka.client

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.MessageID as ProtoMessageID
import com.exactpro.th2.common.grpc.ConnectionID as ProtoConnectionID
import com.exactpro.th2.common.grpc.Direction as ProtoDirection
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage
import com.exactpro.th2.common.grpc.RawMessageMetadata as ProtoRawMessageMetadata
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toByteArray
import com.exactpro.th2.common.utils.message.logId
import com.exactpro.th2.common.utils.message.transport.toProto
import com.google.protobuf.UnsafeByteOperations
import mu.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import java.io.Closeable
import java.time.Duration
import java.time.Instant
import java.util.Collections
import java.util.concurrent.CompletableFuture

abstract class KafkaConnection<MESSAGE, MESSAGE_BUILDER>(
    private val config: Config,
    private val factory: CommonFactory,
    private val messageAcceptor: MessageAcceptor<MESSAGE, MESSAGE_BUILDER>,
    private val eventSender: EventSender,
    kafkaClientsFactory: KafkaClientsFactory
) : Runnable, Closeable {
    protected val bookName = factory.boxConfiguration.bookName
    private val consumer: Consumer<String, ByteArray> = kafkaClientsFactory.getKafkaConsumer()
    private val producer: Producer<String, ByteArray> = kafkaClientsFactory.getKafkaProducer()
    private val pollTimeout = Duration.ofMillis(config.kafkaPollTimeoutMs)

    protected abstract val MESSAGE.logId: String
    protected abstract val MESSAGE.messageSessionAlias: String?
    protected abstract val MESSAGE.rawBody: ByteArray
    protected abstract fun MESSAGE.toProtoMessageId(sessionGroup: String): ProtoMessageID?
    protected abstract fun prepareOutgoingMessage(messageToSend: MESSAGE, book: String): MESSAGE_BUILDER
    protected abstract fun prepareIncomingMessage(alias: String, record: ConsumerRecord<String?, ByteArray>, metadataFields: Map<String, String>): MESSAGE_BUILDER

    fun publish(message: MESSAGE) {
        val alias = message.messageSessionAlias ?: error("Message '${message.logId}' does not contain session alias.")

        val newMessageBuilder = prepareOutgoingMessage(message, bookName)
        val messageFuture = CompletableFuture<Pair<MESSAGE, String>>()

        messageAcceptor.onMessage(newMessageBuilder) { msg, sessionGroup -> messageFuture.complete(msg to sessionGroup) }

        val value = message.rawBody
        val kafkaStream = config.aliasToTopicAndKey[alias] ?: KafkaStream(config.aliasToTopic[alias]?.topic ?: error("Session alias '$alias' not found."), null)
        val kafkaRecord = ProducerRecord<String, ByteArray>(kafkaStream.topic, kafkaStream.key, value)
        producer.send(kafkaRecord) { _, exception: Throwable? ->
            val (outMessage, sessionGroup) = messageFuture.get()
            if (exception == null) {
                val msgText = "Message '${outMessage.logId}' sent to Kafka"
                LOGGER.info(msgText)
                if (config.messagePublishingEvents) {
                    eventSender.onEvent(msgText, "Send message", outMessage.toProtoMessageId(sessionGroup))
                }
            } else {
                throw RuntimeException("Failed to send message '${outMessage.logId}' to Kafka", exception)
            }
        }
    }

    private fun isKafkaAvailable(): Boolean = try {
        consumer.listTopics(pollTimeout)
        true
    } catch (e: TimeoutException) {
        false
    }

    override fun run() = try {
        val startTimestamp = Instant.now().toEpochMilli()
        val topics = config.topicToAlias.keys + config.topicAndKeyToAlias.map { it.key.topic }
        consumer.subscribe(topics)

        if (config.offsetResetOnStart != ResetOffset.NONE) {
            val partitions = topics.asSequence()
                .flatMap { consumer.partitionsFor(it, pollTimeout) }
                .map { TopicPartition(it.topic(), it.partition()) }
                .toList()

            when (config.offsetResetOnStart) {
                ResetOffset.BEGIN -> consumer.seekToBeginning(partitions)
                ResetOffset.END -> consumer.seekToEnd(partitions)
                ResetOffset.MESSAGE -> {
                    if (config.offsetResetMessage >= 0) {
                        partitions.forEach { consumer.seek(it, config.offsetResetMessage) }
                    } else {
                        consumer.endOffsets(partitions)
                            .forEach { consumer.seek(it.key, it.value + config.offsetResetMessage) }
                    }
                }
                ResetOffset.TIME -> {
                    val time = if (config.offsetResetTimeMs >= 0) {
                        config.offsetResetTimeMs
                    } else {
                        System.currentTimeMillis() + config.offsetResetTimeMs
                    }

                    consumer.offsetsForTimes(partitions.associateWith { time }, pollTimeout).forEach {
                        consumer.seek(it.key, it.value.offset())
                    }
                }
                else -> error("Wrong 'offsetResetOnStart' value")
            }
        }

        while (!Thread.currentThread().isInterrupted) {
            val records: ConsumerRecords<String?, ByteArray> = consumer.poll(pollTimeout)

            if (records.isEmpty) {
                if (config.kafkaConnectionEvents && !isKafkaAvailable()) {
                    val failedToConnectMessage = "Failed to connect Kafka"
                    LOGGER.error(failedToConnectMessage)
                    eventSender.onEvent(failedToConnectMessage, CONNECTIVITY_EVENT_TYPE, status = Event.Status.FAILED)

                    while (!Thread.currentThread().isInterrupted && !isKafkaAvailable()) {
                        /* wait for connection */
                    }

                    if (!Thread.currentThread().isInterrupted) {
                        val connectionRestoredMessage = "Kafka connection restored"
                        LOGGER.info(connectionRestoredMessage)
                        eventSender.onEvent(connectionRestoredMessage, CONNECTIVITY_EVENT_TYPE)
                    }
                }
                continue
            }

            LOGGER.trace { "Batch with ${records.count()} records polled from Kafka" }

            val topicsToSkip: MutableSet<String> = HashSet()
            for (record in records) {
                val inactivityPeriod = startTimestamp - record.timestamp()
                if (inactivityPeriod > config.maxInactivityPeriodDuration.inWholeMilliseconds) {
                    val topicToSkip = record.topic()
                    topicsToSkip.add(topicToSkip)
                    consumer.seekToEnd(
                        consumer.partitionsFor(topicToSkip).map { TopicPartition(topicToSkip, it.partition()) }
                    )
                    val msgText = "Inactivity period exceeded ($inactivityPeriod ms). Skipping unread messages in '$topicToSkip' topic."
                    LOGGER.info { msgText }
                    eventSender.onEvent(msgText, CONNECTIVITY_EVENT_TYPE)
                } else {
                    if (record.topic() in topicsToSkip) continue

                    val alias = config.topicToAlias[record.topic()]
                        ?: config.topicAndKeyToAlias[KafkaStream(record.topic(), record.key(), true)]
                        ?: continue

                    val metadataFields = if (config.addExtraMetadata) {
                        hashMapOf<String, String>().apply {
                            put(METADATA_TOPIC, record.topic())
                            record.key()?.let { put(METADATA_KEY, it) }
                            put(METADATA_PARTITION, record.partition().toString())
                            put(METADATA_OFFSET, record.offset().toString())
                            put(METADATA_TIMESTAMP, record.timestamp().toString())
                            record.timestampType()?.let { put(METADATA_TIMESTAMP_TYPE, it.toString()) }
                        }
                    } else {
                        emptyMap()
                    }

                    val builder = prepareIncomingMessage(alias, record, metadataFields)
                    messageAcceptor.onMessage(builder)
                 }
            }

            consumer.commitAsync { offsets: Map<TopicPartition, OffsetAndMetadata>, exception: Exception? ->
                if (exception == null) {
                    LOGGER.trace { "Commit succeed for offsets $offsets" }
                } else {
                    LOGGER.error(exception) { "Commit failed for offsets $offsets" }
                }
            }
        }
    } catch (e: InterruptedException) {
        LOGGER.info("Polling thread interrupted")
    } catch (e: Exception) {
        val errorMessage = "Failed to read messages from Kafka"
        LOGGER.error(errorMessage, e)
        eventSender.onEvent(errorMessage, "Error", exception = e)
    } finally {
        consumer.wakeup()
        consumer.close()
    }

    override fun close() = producer.close()

    companion object {
        private val LOGGER = KotlinLogging.logger {}
        private const val CONNECTIVITY_EVENT_TYPE = "ConnectivityServiceEvent"

        private const val METADATA_TOPIC = "th2.kafka.topic"
        private const val METADATA_KEY = "th2.kafka.key"
        private const val METADATA_PARTITION = "th2.kafka.partition"
        private const val METADATA_OFFSET = "th2.kafka.offset"
        private const val METADATA_TIMESTAMP = "th2.kafka.timestamp"
        private const val METADATA_TIMESTAMP_TYPE = "th2.kafka.timestampType"

        fun createTopics(config: Config) {
            if (config.topicsToCreate.isEmpty()) return

            AdminClient.create(
                mapOf(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to config.bootstrapServers,
                    AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG to config.reconnectBackoffMs,
                    AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG to config.reconnectBackoffMaxMs,
                )
            ).use { adminClient ->
                val currentTopicList = adminClient.listTopics().names().get()
                config.topicsToCreate.forEach { topic ->
                    if (topic in currentTopicList) {
                        LOGGER.info { "Topic '$topic' already exists" }
                    } else {
                        runCatching {
                            val result = adminClient.createTopics(
                                Collections.singleton(
                                    NewTopic(topic, config.newTopicsPartitions, config.newTopicsReplicationFactor)
                                )
                            )
                            result.all().get()
                        }.onSuccess {
                            LOGGER.info { "Topic '$topic' created" }
                        }.onFailure { ex ->
                            throw RuntimeException("Failed to create topic '$topic'", ex)
                        }
                    }
                }
            }
        }
    }
}

class ProtoKafkaConnection(
    private val config: Config,
    private val factory: CommonFactory,
    messageAcceptor: MessageAcceptor<ProtoRawMessage, ProtoRawMessage.Builder>,
    eventSender: EventSender,
    kafkaClientsFactory: KafkaClientsFactory
) : KafkaConnection<ProtoRawMessage, ProtoRawMessage.Builder>(config, factory, messageAcceptor, eventSender, kafkaClientsFactory) {

    override val ProtoRawMessage.logId: String get() = id.logId
    override val ProtoRawMessage.messageSessionAlias: String? get() = id.connectionId.sessionAlias
    override val ProtoRawMessage.rawBody: ByteArray get() = body.toByteArray()
    override fun ProtoRawMessage.toProtoMessageId(sessionGroup: String): ProtoMessageID = id

    override fun prepareOutgoingMessage(messageToSend: ProtoRawMessage, book: String): ProtoRawMessage.Builder {
        val messageIdBuilder = messageToSend.id.toBuilder().apply {
            direction = ProtoDirection.SECOND
        }

        return ProtoRawMessage.newBuilder()
            .setMetadata(messageToSend.metadata.toBuilder().setId(messageIdBuilder))
            .setBody(messageToSend.body)
    }

    override fun prepareIncomingMessage(
        alias: String,
        record: ConsumerRecord<String?, ByteArray>,
        metadataFields: Map<String, String>
    ): ProtoRawMessage.Builder {
        val messageID = ProtoMessageID.newBuilder()
            .setDirection(ProtoDirection.FIRST)
            .setConnectionId(ProtoConnectionID.newBuilder().setSessionAlias(alias))

        val metadata = ProtoRawMessageMetadata.newBuilder()
            .setId(messageID)
            .putAllProperties(metadataFields)

        return ProtoRawMessage.newBuilder()
            .setMetadata(metadata)
            .setBody(UnsafeByteOperations.unsafeWrap(record.value()))
    }
}

class TransportKafkaConnection(
    private val config: Config,
    private val factory: CommonFactory,
    messageAcceptor: MessageAcceptor<RawMessage, RawMessage.Builder>,
    eventSender: EventSender,
    kafkaClientsFactory: KafkaClientsFactory
) : KafkaConnection<RawMessage, RawMessage.Builder>(config, factory, messageAcceptor, eventSender, kafkaClientsFactory) {
    override val RawMessage.logId: String // TODO: move to utils?
        get() = "${id.sessionAlias}:${id.direction.toString().lowercase()}:${id.timestamp}:${id.sequence}:${id.subsequence.joinToString(".")}"
    override val RawMessage.messageSessionAlias: String? get() = id.sessionAlias
    override val RawMessage.rawBody: ByteArray get() = body.toByteArray()
    override fun RawMessage.toProtoMessageId(sessionGroup: String): ProtoMessageID = id.toProto(bookName, sessionGroup)

    override fun prepareOutgoingMessage(messageToSend: RawMessage, book: String): RawMessage.Builder = RawMessage.builder()
        .setId(messageToSend.id.toBuilder().setDirection(Direction.OUTGOING).build())
        .setMetadata(messageToSend.metadata)
        .setBody(messageToSend.body)

    override fun prepareIncomingMessage(
        alias: String,
        record: ConsumerRecord<String?, ByteArray>,
        metadataFields: Map<String, String>
    ): RawMessage.Builder = RawMessage.builder()
        .setId(
            MessageId.builder()
                .setDirection(Direction.INCOMING)
                .setSessionAlias(alias)
                .build()
        )
        .setMetadata(metadataFields)
        .setBody(record.value())
}