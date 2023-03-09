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
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

class Config(
    /**
     * Matches th2 sessions with Kafka topics
     */
    val aliasToTopic: Map<String, KafkaTopic> = emptyMap(),

    /**
     * Matches th2 sessions with Kafka topics and keys
     */
    val aliasToTopicAndKey: Map<String, KafkaStream> = emptyMap(),

    /**
     * Matches th2 sessions with session groups
     * Key: session group, value: list of session aliases
     */
    sessionGroups: Map<String, List<String>> = emptyMap(),

    /**
     * This session group will be set for outgoing messages whose
     * session alias does not exist in the "sessionGroups" parameter.
     */
    defaultSessionGroup: String? = null,

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
     * The size of one batch (number of messages)
     */
    val batchSize: Int = 100,

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
     * Kafka producer batch size in bytes
     */
    val kafkaBatchSize: Int = 16384,

    /**
     * The upper bound on the delay for batching
     */
    val kafkaLingerMillis: Int = 20,

    /**
     * The maximum amount of time in milliseconds to backoff/wait when reconnecting to a broker that
     * has repeatedly failed to connect. If provided, the backoff per host will increase
     * exponentially for each consecutive connection failure, up to this maximum.
     * Once the maximum is reached, reconnection attempts will continue periodically with
     * this fixed rate. To avoid connection storms, a randomization factor of 0.2 will be applied to
     * the backoff resulting in a random range between 20% below and 20% above the computed value
     */
    val reconnectBackoffMaxMs: Int = 1000,

    /**
     * Generate TH2 event on connect|disconnect Kafka
     */
    val kafkaConnectionEvents: Boolean = false,

    /**
     * Generate TH2 event on successful message publishing
     */
    val messagePublishingEvents: Boolean = false,

    /**
     * Add extra metadata to messages (like topic, key. offset, original timestamp ...)
     */
    val addExtraMetadata: Boolean = false,

    /**
     * Protocol used to communicate with brokers
     */
    @JsonProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
    val kafkaSecurityProtocol: String? = null,

    /**
     * The Kerberos principal name that Kafka runs as
     */
    @JsonProperty(SaslConfigs.SASL_KERBEROS_SERVICE_NAME)
    val kafkaSaslKerberosServiceName: String? = null,

    /**
     * SASL mechanism used for client connections
     */
    @JsonProperty(SaslConfigs.SASL_MECHANISM)
    val kafkaSaslMechanism: String? = null,

    /**
     * JAAS login context parameters for SASL connections in the format used by JAAS configuration files
     */
    @JsonProperty(SaslConfigs.SASL_JAAS_CONFIG)
    val kafkaSaslJaasConfig: String? = null,

    val createTopics: Boolean = false,
    val topicsToCreate: List<String> = emptyList(),
    val newTopicsPartitions: Int = 1,
    val newTopicsReplicationFactor: Short = 1
) {
    @JsonIgnore
    val maxInactivityPeriod: Duration = maxInactivityPeriodUnit.toMillis(maxInactivityPeriod).toDuration(DurationUnit.MILLISECONDS)

    @JsonIgnore
    val topicToAlias: Map<String, String> = aliasToTopic.asSequence()
        .filter { it.value.subscribe }
        .map {
            require(it.value.topic.isNotEmpty()) { "Topic can't be empty string in 'topicToAlias'" }
            require(it.key.isNotEmpty()) { "Alias can't be empty string in 'topicToAlias'" }
            it.value.topic to it.key
        }.toMap()

    @JsonIgnore
    val topicAndKeyToAlias: Map<KafkaStream, String> = aliasToTopicAndKey.asSequence()
        .filter { it.value.subscribe }
        .map {
            require(it.value.topic.isNotEmpty()) { "Topic can't be empty string in 'topicAndKeyToAlias'" }
            require(it.value.key?.isNotEmpty() ?: true) { "Key can't be empty string in 'topicAndKeyToAlias'" }
            require(it.key.isNotEmpty()) { "Alias can't be empty string in 'topicAndKeyToAlias'" }
            it.value to it.key
        }.toMap()

    @JsonIgnore
    val aliasToSessionGroup: Map<String, String> = sessionGroups.asSequence().flatMap { (group, aliases) ->
        require(group.isNotEmpty()) { "Session group name can't be empty string in 'aliasToSessionGroup'" }
        aliases.map { alias ->
            require(alias.isNotEmpty()) { "Alias can't be empty string in 'aliasToSessionGroup'" }
            alias to group
        }
    }.toMap().withDefault { alias -> defaultSessionGroup ?: alias }

    init {
        require(aliasToTopicAndKey.isNotEmpty() || aliasToTopic.isNotEmpty()) { "'aliasToTopicAndKey' and 'aliasToTopic' can't both be empty" }

        val duplicatedAliases = aliasToTopicAndKey.keys.intersect(aliasToTopic.keys)
        require(duplicatedAliases.isEmpty()) { "'aliasToTopicAndKey' and 'aliasToTopic' can't contain the same aliases: $duplicatedAliases" }

        require(aliasToTopic.asSequence().map { it.value.topic }.noDuplicates()) { "Duplicated topic in 'aliasToTopic'" }
        require(aliasToTopicAndKey.asSequence().map { it.value.topic to it.value.key }.noDuplicates()) { "Duplicated data stream in 'aliasToTopicAndKey'" }
        val duplicatedTopics = aliasToTopicAndKey.map { it.value.topic }.intersect(aliasToTopic.values.map { it.topic }.toSet())
        require(duplicatedTopics.isEmpty()) { "'aliasToTopicAndKey' and 'aliasToTopic' can't contain the same topics: $duplicatedTopics" }

        require(aliasToSessionGroup.keys.size == sessionGroups.values.sumOf { it.size }) { "Duplicated alias in 'sessionGroups'" }

        require(reconnectBackoffMaxMs > 0) { "'reconnectBackoffMaxMs' must be positive. Please, check the configuration. $reconnectBackoffMaxMs" }
        require(reconnectBackoffMs > 0) { "'reconnectBackoffMs' must be positive. Please, check the configuration. $reconnectBackoffMs" }
        require(batchSize > 0) { "'batchSize' must be positive. Please, check the configuration. $batchSize" }
        require(maxInactivityPeriod > 0) { "'maxInactivityPeriod' must be positive. Please, check the configuration. $maxInactivityPeriod" }
        require(timeSpan > 0) { "'timeSpan' must be positive. Please, check the configuration. $timeSpan" }
        require(kafkaBatchSize > 0) { "'kafkaBatchSize' must be positive. Please, check the configuration. $kafkaBatchSize" }
        require(kafkaLingerMillis > 0) { "'kafkaLingerMillis' must be positive. Please, check the configuration. $kafkaLingerMillis" }
    }

    private fun <T> Sequence<T>.noDuplicates(): Boolean {
        val seen: MutableSet<T> = hashSetOf()
        return all { seen.add(it) }
    }
}

data class KafkaTopic(
    @JsonProperty(required = true) val topic: String,
    @JsonProperty(defaultValue = "false") val subscribe: Boolean = false
)

data class KafkaStream(
    @JsonProperty(required = true) val topic: String,
    @JsonProperty(required = true) val key: String?,
    @JsonProperty(defaultValue = "false") val subscribe: Boolean = false
)