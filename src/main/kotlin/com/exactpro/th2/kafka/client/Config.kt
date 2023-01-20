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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

class Config(
    /**
     * Match sessionAlias with topic and key
     */
    @JsonProperty(required = true)
    val aliasToKafkaStream: Map<String, KafkaStream>,

    val sessionGroup: String? = null,

    /**
     * URL of one of the Kafka brokers which you give to fetch the initial metadata about your Kafka cluster
     */
    val bootstrapServers: String = "localhost:9092",

    /**
     * Name of the consumer group a Kafka consumer belongs to
     */
    val groupId: String = "",

    /**
     * If the period of inactivity is longer than this time, then start reading Kafka messages from the current moment
     */
    maxInactivityPeriod: Long = 8L,

    /**
     * Time unit for `maxInactivityPeriod` classification
     */
    maxInactivityPeriodUnit: TimeUnit = TimeUnit.HOURS,

    /**
     * The size of one batch
     */
    val batchSize: Long = 100L,

    /**
     * The period router collects messages before it should be sent
     */
    val timeSpan: Long = 1000L,

    /**
     * The unit of time which applies to the `timeSpan` argument
     */
    val timeSpanUnit: TimeUnit = TimeUnit.MILLISECONDS,

    /**
     * The amount of time in milliseconds to wait before attempting to reconnect to a given host
     */
    val reconnectBackoffMs: Int = 50,

    /**
     * The maximum amount of time in milliseconds to backoff/wait when reconnecting to a broker that
     * has repeatedly failed to connect. If provided, the backoff per host will increase
     * exponentially for each consecutive connection failure, up to this maximum.
     * Once the maximum is reached, reconnection attempts will continue periodically with
     * this fixed rate. To avoid connection storms, a randomization factor of 0.2 will be applied to
     * the backoff resulting in a random range between 20% below and 20% above the computed value
     */
    val reconnectBackoffMaxMs: Int = 1000
) {
    @JsonIgnore
    val maxInactivityPeriod: Duration = maxInactivityPeriodUnit.toMillis(maxInactivityPeriod).toDuration(DurationUnit.MILLISECONDS)

    @JsonIgnore
    val kafkaStreamToAlias: Map<KafkaStream, String> = aliasToKafkaStream.map { it.value to it.key }.toMap()

    init {
        require(aliasToKafkaStream.isNotEmpty()) { "'aliasToKafkaStream' can't be empty" }
        require(kafkaStreamToAlias.size == aliasToKafkaStream.size) { "Duplicated data stream in 'aliasToKafkaStream'" }
        require(reconnectBackoffMaxMs > 0) { "'reconnectBackoffMaxMs' must be positive. Please, check the configuration. $reconnectBackoffMaxMs" }
        require(reconnectBackoffMs > 0) { "'reconnectBackoffMs' must be positive. Please, check the configuration. $reconnectBackoffMs" }
        require(batchSize > 0) { "'batchSize' must be positive. Please, check the configuration. $batchSize" }
        require(maxInactivityPeriod > 0) { "'maxInactivityPeriod' must be positive. Please, check the configuration. $maxInactivityPeriod" }
        require(timeSpan > 0) { "'timeSpan' must be positive. Please, check the configuration. $timeSpan" }
    }
}

data class KafkaStream(val topic: String, val key: String?)