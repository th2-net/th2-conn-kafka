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
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.google.protobuf.UnsafeByteOperations
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.time.Instant
import java.util.HashSet
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class KafkaConnection(
    private val config: Config,
    private val factory: CommonFactory,
    private val messageProcessor: MessageProcessor,
    private val eventSender: EventSender
) : Runnable {
    private val consumer: Consumer<String, ByteArray> = KafkaConsumer(
        Properties().apply {
            putAll(mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to config.bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to config.groupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG to config.reconnectBackoffMs,
                ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG to config.reconnectBackoffMaxMs
            ))
        }
    )

    private val producer: Producer<String, ByteArray> = KafkaProducer(
        Properties().apply {
            putAll(mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to config.bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG to config.reconnectBackoffMs,
                ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG to config.reconnectBackoffMaxMs
            ))
        }
    )

    private fun createSequence(): () -> Long = Instant.now().run {
        AtomicLong(epochSecond * TimeUnit.SECONDS.toNanos(1) + nano)
    }::incrementAndGet

    private val firstSequence: () -> Long = createSequence()
    private val secondSequence: () -> Long = createSequence()

    fun publish(message: RawMessage) {
        val alias = message.metadata.id.connectionId.sessionAlias
        val topic = config.aliasToTopic[alias] ?: error("Session alias '$alias' not found.")
        val value = message.body.toByteArray()

        val messageID = message.metadata.id.toBuilder()
            .setBookName(factory.boxConfiguration.bookName)
            .setDirection(Direction.SECOND)
            .setSequence(secondSequence())
            .setTimestamp(Instant.now().toTimestamp())
            .build()

        messageProcessor.process(
            RawMessage.newBuilder()
                .setMetadata(message.metadata.toBuilder().setId(messageID))
                .setBody(message.body)
                .build()
        )

        val kafkaRecord = ProducerRecord<String, ByteArray>(topic, value)
        producer.send(kafkaRecord) { _, exception: Throwable? ->
            when (exception) {
                null -> {
                    val msgText = "Message '${message.metadata.id}' sent to Kafka"
                    LOGGER.info(msgText)
                    eventSender.onEvent(msgText, "Send message", message)
                }
                else -> {
                    val msgText = "Failed to send message '${message.metadata.id}' to Kafka"
                    LOGGER.error(msgText, exception)
                    eventSender.onEvent(msgText, "SendError", message, exception)
                }
            }
        }
    }

    override fun run() = try {
        val startTimestamp = Instant.now().toEpochMilli()
        consumer.subscribe(config.topicToAlias.keys)

        while (!Thread.currentThread().isInterrupted) {
            val records: ConsumerRecords<String?, ByteArray> = consumer.poll(POLL_TIMEOUT)

            if (records.isEmpty) continue

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
                }
            }

            for (record in records) {
                if (record.topic() in topicsToSkip) continue

                val messageID = factory.newMessageIDBuilder()
                    .setConnectionId(
                        ConnectionID.newBuilder()
                            .setSessionAlias(config.topicToAlias[record.topic()])
                            .apply { config.sessionGroup?.let { sessionGroup = it } }
                    )
                    .setDirection(Direction.FIRST)
                    .setSequence(firstSequence())
                    .setTimestamp(Instant.now().toTimestamp())
                    .build()
                messageProcessor.process(
                    RawMessage.newBuilder()
                        .setMetadata(RawMessageMetadata.newBuilder().setId(messageID))
                        .setBody(UnsafeByteOperations.unsafeWrap(record.value()))
                        .build()
                )
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

    companion object {
        private val LOGGER = KotlinLogging.logger {}
        private val POLL_TIMEOUT = Duration.ofMillis(100L)
    }
}