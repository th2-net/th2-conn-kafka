/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaConfigurationSettings {

    /**
     * Match topic with sessionAlias
     */
    @JsonProperty(required = true, value = "topicToAlias")
    private Map<String, String> topicToSessionAliasMap = new HashMap<>();

    /**
     * URL of one of the Kafka brokers which you give to fetch the initial metadata about your Kafka cluster
     */
    private String bootstrapServers = "localhost:9092";

    /**
     * Name of the consumer group a Kafka consumer belongs to
     */
    private String groupId = "";

    /**
     * If the period of inactivity is longer than this time, then start reading Kafka messages from the current moment
     */
    private long acceptableBreak = 8;

    /**
     * Time unit for {@code acceptableBreak} classification
     */
    private TimeUnit acceptableBreakTimeUnit = TimeUnit.HOURS;

    private int batchSize = 100;

    /**
     * The period router collects messages before it should be sent
     */
    private long timespan = 1000;

    /**
     * The unit of time which applies to the {@code timespan} argument
     */
    private TimeUnit timespanUnit = TimeUnit.MILLISECONDS;

    /**
     * The amount of time in milliseconds to wait before attempting to reconnect to a given host
     */
    private int reconnectBackoffMs = 50;

    /**
     * The maximum amount of time in milliseconds to backoff/wait when reconnecting to a broker that
     * has repeatedly failed to connect. If provided, the backoff per host will increase
     * exponentially for each consecutive connection failure, up to this maximum.
     * Once the maximum is reached, reconnection attempts will continue periodically with
     * this fixed rate. To avoid connection storms, a randomization factor of 0.2 will be applied to
     * the backoff resulting in a random range between 20% below and 20% above the computed value
     */
    private int reconnectBackoffMaxMs = 1000;

    public Map<String, String> getTopicToSessionAliasMap() {
        return topicToSessionAliasMap;
    }


    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public long getAcceptableBreak() {
        return acceptableBreak;
    }

    public TimeUnit getAcceptableBreakTimeUnit() {
        return acceptableBreakTimeUnit;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getTimespan() {
        return timespan;
    }

    public TimeUnit getTimespanUnit() {
        return timespanUnit;
    }

    public int getReconnectBackoffMs() {
        return reconnectBackoffMs;
    }

    public int getReconnectBackoffMaxMs() {
        return reconnectBackoffMaxMs;
    }
}
