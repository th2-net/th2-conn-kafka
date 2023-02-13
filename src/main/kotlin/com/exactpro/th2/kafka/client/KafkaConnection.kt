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

import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.common.message.logId
import com.google.protobuf.UnsafeByteOperations
import mu.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import java.io.Closeable
import java.time.Duration
import java.time.Instant
import java.util.HashSet
import java.util.Collections

class KafkaConnection(
    private val config: Config,
    private val messageProcessor: RawMessageProcessor,
    private val eventSender: EventSender,
    kafkaClientsFactory: KafkaClientsFactory
) : Runnable, Closeable {
    private val consumer: Consumer<String, ByteArray> = kafkaClientsFactory.getKafkaConsumer()
    private val producer: Producer<String, ByteArray> = kafkaClientsFactory.getKafkaProducer()

    fun publish(message: RawMessage) {
        val alias = message.metadata.id.connectionId.sessionAlias
        val kafkaStream = config.aliasToTopicAndKey[alias] ?: KafkaStream(config.aliasToTopic[alias]?.topic ?: error("Session alias '$alias' not found."), null)
        val value = message.body.toByteArray()
        val messageIdBuilder = message.metadata.id.toBuilder().apply {
            direction = Direction.SECOND
            setConnectionId(connectionIdBuilder.setSessionGroup(config.aliasToSessionGroup.getValue(alias)))
        }

        messageProcessor.onMessage(
            RawMessage.newBuilder()
                .setMetadata(message.metadata.toBuilder().setId(messageIdBuilder))
                .setBody(message.body)
        )

        val kafkaRecord = ProducerRecord<String, ByteArray>(kafkaStream.topic, kafkaStream.key, value)
        producer.send(kafkaRecord) { _, exception: Throwable? ->
            if (exception == null) {
                val msgText = "Message '${message.logId}' sent to Kafka"
                LOGGER.info(msgText)
                eventSender.onEvent(msgText, "Send message", message)
            } else {
                throw RuntimeException("Failed to send message '${message.logId}' to Kafka", exception)
            }
        }
    }

    override fun run() = try {
        val startTimestamp = Instant.now().toEpochMilli()
        consumer.subscribe(config.topicToAlias.keys + config.topicAndKeyToAlias.map { it.key.topic })

        while (!Thread.currentThread().isInterrupted) {
            val records: ConsumerRecords<String?, ByteArray> = consumer.poll(POLL_TIMEOUT)
            if (records.isEmpty) continue
            LOGGER.trace { "Batch with ${records.count()} records polled from Kafka" }

            val topicsToSkip: MutableSet<String> = HashSet()
            for (record in records) {
                val inactivityPeriod = startTimestamp - record.timestamp()
                if (inactivityPeriod > config.maxInactivityPeriod.inWholeMilliseconds) {
                    val topicToSkip = record.topic()
                    topicsToSkip.add(topicToSkip)
                    consumer.seekToEnd(
                        consumer.partitionsFor(topicToSkip).map { TopicPartition(topicToSkip, it.partition()) }
                    )
                    val msgText =
                        "Inactivity period exceeded ($inactivityPeriod ms). Skipping unread messages in '$topicToSkip' topic."
                    LOGGER.info { msgText }
                    eventSender.onEvent(msgText, "ConnectivityServiceEvent")
                } else {
                    if (record.topic() in topicsToSkip) continue

                    val alias = config.topicToAlias[record.topic()]
                        ?: config.topicAndKeyToAlias[KafkaStream(record.topic(), record.key(), true)]
                        ?: continue

                    val messageID = MessageID.newBuilder()
                        .setConnectionId(
                            ConnectionID.newBuilder()
                                .setSessionAlias(alias)
                                .setSessionGroup(config.aliasToSessionGroup.getValue(alias))
                        )
                        .setDirection(Direction.FIRST)

                    messageProcessor.onMessage(RawMessage.newBuilder()
                        .setMetadata(RawMessageMetadata.newBuilder().setId(messageID))
                        .setBody(UnsafeByteOperations.unsafeWrap(record.value()))
                    )
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
    } catch (e: Exception) {
        val errorMessage = "Failed to read messages from Kafka"
        LOGGER.error(errorMessage, e)
        eventSender.onEvent(errorMessage, "Error", exception = e)
    } finally {
        consumer.wakeup()
        consumer.close()
    }

    override fun close() {
        producer.close()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
        private val POLL_TIMEOUT = Duration.ofMillis(100L)

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