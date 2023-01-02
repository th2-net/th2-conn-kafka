/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.time.Instant
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

class KafkaConnection(private val factory: CommonFactory, private val messageProcessor: MessageProcessor) : Runnable {
    private val settings = factory.getCustomConfiguration(Config::class.java)
    private val consumer: Consumer<String, ByteArray> = KafkaConsumer(
        Properties().apply {
            putAll(mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to settings.bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to settings.groupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG to settings.reconnectBackoffMs,
                ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG to settings.reconnectBackoffMaxMs
            ))
        }
    )
    private val firstSequence = Instant.now().let {
        AtomicLong(it.epochSecond * NANOSECONDS_IN_SECOND + it.nano)
    }

    override fun run() {
        try {
            val topics = settings.topicToAlias.keys
            consumer.subscribe(topics)
            var records: ConsumerRecords<String, ByteArray>
            do {
                records = consumer.poll(Duration.ofMillis(DURATION_IN_MILLIS))
            } while (records.isEmpty)
            for (record in records) {
                if (System.currentTimeMillis() - record.timestamp() > convertedTime) {
                    consumer.seekToEnd(topicPartitions)
                    try {
                        consumer.commitSync()
                    } catch (e: CommitFailedException) {
                        LOGGER.error(e) { "Commit failed" }
                    }
                    records = consumer.poll(Duration.ofMillis(DURATION_IN_MILLIS))
                }
                break
            }
            val currentThread = Thread.currentThread()
            while (!currentThread.isInterrupted) {
                for (record in records) {
                    val messageID = factory.newMessageIDBuilder()
                        .setConnectionId(
                            ConnectionID.newBuilder()
                                .setSessionAlias(settings.topicToAlias[record.topic()])
                                .setSessionGroup(settings.sessionGroup)
                                .build()
                        )
                        .setDirection(Direction.FIRST)
                        .setSequence(sequence)
                        .setTimestamp(Instant.ofEpochSecond(record.timestamp()).toTimestamp())
                        .build()
                    messageProcessor.process(
                        RawMessage.newBuilder()
                            .setMetadata(RawMessageMetadata.newBuilder().setId(messageID))
                            .setBody(ByteString.copyFrom(record.value()))
                            .build()
                    )
                }
                if (!records.isEmpty) {
                    consumer.commitAsync { offsets: Map<TopicPartition?, OffsetAndMetadata?>?, exception: Exception? ->
                        if (exception != null) {
                            LOGGER.error(exception) { "Commit failed for offsets $offsets" }
                        }
                    }
                }
                records = consumer.poll(Duration.ofMillis(DURATION_IN_MILLIS))
            }
        } catch (e: RuntimeException) {
            LOGGER.error(e) { "Failed to read message from kafka" }
        } finally {
            consumer.wakeup()
            consumer.close()
        }
    }

    private val sequence: Long
        get() = firstSequence.incrementAndGet()
    private val convertedTime = settings.acceptableBreakTimeUnit.toMillis(settings.acceptableBreak)
    private val topicPartitions: List<TopicPartition>
        get() {
            val allPartitions: MutableList<TopicPartition> = ArrayList()
            for (topic in settings.topicToAlias.keys) {
                val partitions = consumer.partitionsFor(topic)
                for (par in partitions) {
                    allPartitions.add(TopicPartition(topic, par.partition()))
                }
            }
            return allPartitions
        }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
        private const val NANOSECONDS_IN_SECOND = 1_000_000_000L
        private const val DURATION_IN_MILLIS = 100L
    }
}