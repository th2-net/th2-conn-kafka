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
import com.google.protobuf.ByteString
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
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
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class KafkaConnection(
    private val factory: CommonFactory,
    private val messageProcessor: MessageProcessor,
    private val eventSender: EventSender
) : Runnable {
    private val config = factory.getCustomConfiguration(Config::class.java)
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

    private val firstSequence = createSequence()
    private val secondSequence = createSequence()

    fun publish(message: RawMessage) {
        val alias = message.metadata.id.connectionId.sessionAlias
        val topic = config.aliasToTopic[alias] ?: error("Session alias not found.")
        val value = message.body.toByteArray()

        val messageID = message.metadata.id.toBuilder()
            .setBookName(factory.boxConfiguration.bookName)
            .setDirection(Direction.SECOND)
            .setSequence(secondSequence())
            .setTimestamp(Instant.now().toTimestamp())
            .build()

        messageProcessor.process(
            RawMessage.newBuilder()
                .setMetadata(RawMessageMetadata.newBuilder().setId(messageID))
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

    override fun run() {
        try {
            consumer.subscribe(config.topicToAlias.keys)
            while (!Thread.currentThread().isInterrupted) {
                val records = consumer.poll(Duration.ofMillis(DURATION_IN_MILLIS))

                if (!records.isEmpty) {
                    val inactivityPeriod = System.currentTimeMillis() - records.first().timestamp()
                    if (inactivityPeriod > config.maxInactivityPeriodMillis) {
                        consumer.seekToEnd(getAllPartitions())
                        val msgText = "Inactivity period exceeded ($inactivityPeriod ms). Skipping unread messages."
                        LOGGER.info { msgText }
                        eventSender.onEvent(msgText, "ConnectivityServiceEvent")
                    } else {
                        for (record in records) {
                            val messageID = factory.newMessageIDBuilder()
                                .setConnectionId(
                                    ConnectionID.newBuilder()
                                        .setSessionAlias(config.topicToAlias[record.topic()])
                                        .setSessionGroup(config.sessionGroup)
                                        .build()
                                )
                                .setDirection(Direction.FIRST)
                                .setSequence(firstSequence())
                                .setTimestamp(Instant.now().toTimestamp())
                                .build()
                            messageProcessor.process(
                                RawMessage.newBuilder()
                                    .setMetadata(RawMessageMetadata.newBuilder().setId(messageID))
                                    .setBody(ByteString.copyFrom(record.value()))
                                    .build()
                            )
                        }
                    }

                    consumer.commitAsync { offsets: Map<TopicPartition, OffsetAndMetadata>, exception: Exception? ->
                        if (exception != null) {
                            LOGGER.error(exception) { "Commit failed for offsets $offsets" }
                        }
                    }
                }
            }
        } catch (e: RuntimeException) {
            val errorMessage = "Failed to read messages from kafka"
            LOGGER.error(errorMessage, e)
            eventSender.onEvent(errorMessage, "Error", exception =  e)
        } finally {
            consumer.wakeup()
            consumer.close()
        }
    }

    private fun getAllPartitions(): List<TopicPartition> {
        val partitions: MutableList<TopicPartition> = ArrayList()
        for (topic in config.topicToAlias.keys) {
            for (partInfo in consumer.partitionsFor(topic)) {
                partitions += TopicPartition(topic, partInfo.partition())
            }
        }
        return partitions
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
        private const val NANOSECONDS_IN_SECOND = 1_000_000_000L
        private const val DURATION_IN_MILLIS = 100L
    }
}