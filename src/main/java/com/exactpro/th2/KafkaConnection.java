/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.configuration.KafkaConfigurationSettings;
import com.exactpro.th2.processor.MessageProcessor;
import com.google.protobuf.ByteString;

public class KafkaConnection implements Runnable {
    private static final long NANOSECONDS_IN_SECOND = 1_000_000_000L;
    private static final long DURATION_IN_MILLIS = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnection.class);

    private final MessageProcessor messageProcessor;
    private final Consumer<String, byte[]> consumer;
    private final KafkaConfigurationSettings settings;
    private final AtomicLong firstSequence;

    public KafkaConnection(KafkaConfigurationSettings settings, MessageProcessor messageProcessor) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, settings.getGroupId());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, settings.getReconnectBackoffMs());
        properties.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, settings.getReconnectBackoffMaxMs());
        consumer = new KafkaConsumer<>(properties);
        this.messageProcessor = messageProcessor;
        this.settings = settings;
        Instant now = Instant.now();
        firstSequence = new AtomicLong(now.getEpochSecond() * NANOSECONDS_IN_SECOND + now.getNano());
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(settings.getTopicToSessionAliasMap().keySet());
            ConsumerRecords<String, byte[]> records;
            do {
                records = consumer.poll(Duration.ofMillis(DURATION_IN_MILLIS));
            } while (records.isEmpty());
            for (ConsumerRecord<String, byte[]> record : records) {
                if (System.currentTimeMillis() - record.timestamp() > getConvertedTime()) {
                    consumer.seekToEnd(getTopicPartitions());
                    try {
                        consumer.commitSync();
                    } catch (CommitFailedException e) {
                        LOGGER.error("Commit failed", e);
                    }
                    records = consumer.poll(Duration.ofMillis(DURATION_IN_MILLIS));
                }
                break;
            }
            Thread currentThread = Thread.currentThread();

            while (!currentThread.isInterrupted()) {
                for (ConsumerRecord<String, byte[]> record : records) {
                    MessageID messageID = MessageID.newBuilder()
                            .setConnectionId(ConnectionID.newBuilder()
                                    .setSessionAlias(settings.getTopicToSessionAliasMap().get(record.topic()))
                                    .build())
                            .setDirection(Direction.FIRST)
                            .setSequence(getSequence())
                            .build();
                    messageProcessor.process(RawMessage.newBuilder()
                            .setMetadata(RawMessageMetadata.newBuilder()
                                    .setId(messageID)
                                    .setTimestamp(MessageUtils.toTimestamp(Instant.ofEpochSecond(record.timestamp())))
                                    .build())
                            .setBody(ByteString.copyFrom(record.value()))
                            .build());

                }
                if (!records.isEmpty()) {
                    consumer.commitAsync(new OffsetCommitCallback() {
                        public void onComplete(Map<TopicPartition,
                                OffsetAndMetadata> offsets, Exception exception) {
                            if (exception != null) {
                                LOGGER.error("Commit failed for offsets {}", offsets, exception);
                            }
                        }
                    });
                }
                records = consumer.poll(Duration.ofMillis(DURATION_IN_MILLIS));
            }
        } catch (RuntimeException e) {
            LOGGER.error("Failed to read message from kafka", e);
        } finally {
            consumer.wakeup();
            consumer.close();
        }
    }

    private long getSequence() {
        return firstSequence.incrementAndGet();
    }

    public long getConvertedTime() {
        return settings.getAcceptableBreakTimeUnit().toMillis(settings.getAcceptableBreak());
    }

    private List<TopicPartition> getTopicPartitions() {
        List<TopicPartition> allPartitions = new ArrayList<>();
        for (String topic : settings.getTopicToSessionAliasMap().keySet()) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            for (PartitionInfo par : partitions) {
                allPartitions.add(new TopicPartition(topic, par.partition()));
            }
        }
        return allPartitions;
    }
}
