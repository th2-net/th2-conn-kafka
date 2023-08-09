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
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

class Config(

    val topics: Topics,

    /**
     * This session group will be set for outgoing messages whose
     * session alias does not exist in the "sessionGroups" parameter
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
     * The maximum size of one event batch in bytes
     */
    val eventBatchMaxBytes: Long = 512 * 1024,

    /**
     * The maximum size of one event batch in events
     */
    val eventBatchMaxEvents: Int = 2000,

    /**
     * The period event router collects messages before it should be sent
     */
    val eventBatchTimeSpan: Long = 1000L,

    /**
     * The unit of time which applies to the `timeSpan` and 'eventBatchTimeSpan' argument
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
     * Kafka poll request timeout
     */
    val kafkaPollTimeoutMs: Long = 1000,

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
     * Auto offset reset policy. Possible values: "latest" (default), "earliest"
     */
    @JsonProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
    val kafkaAutoOffsetReset: String = "latest",

    val offsetResetOnStart: ResetOffset = ResetOffset.NONE,
    val offsetResetTimeMs: Long = 0,
    val offsetResetMessage: Long = 0,

    /**
     * Protocol used to communicate with brokers
     */
    @JsonProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
    val kafkaSecurityProtocol: String? = null,

    /**
     * The location of the trust store file
     */
    @JsonProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)
    val kafkaSecurityTruststoreLocation: String? = null,

    /**
     * The password for the trust store file
     */
    @JsonProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
    val kafkaSecurityTruststorePassword: String? = null,

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

    val extraConsumerProps: Map<String, String> = emptyMap(),
    val extraProducerProps: Map<String, String> = emptyMap(),

    val createTopics: Boolean = false,
    val topicsToCreate: List<String> = emptyList(),
    val newTopicsPartitions: Int = 1,
    val newTopicsReplicationFactor: Short = 1
) {
    init {
        checkForDuplicatedAliases(topics.publish)
        checkForDuplicatedAliases(topics.subscribe)
        checkForDuplicatedTopics(topics.publish)
        checkForDuplicatedTopics(topics.subscribe)

        require(reconnectBackoffMaxMs > 0) { "'reconnectBackoffMaxMs' must be positive. Please, check the configuration. $reconnectBackoffMaxMs" }
        require(reconnectBackoffMs > 0) { "'reconnectBackoffMs' must be positive. Please, check the configuration. $reconnectBackoffMs" }
        require(kafkaPollTimeoutMs > 0) { "'kafkaPollTimeoutMs' must be positive. Please, check the configuration. $kafkaPollTimeoutMs" }
        require(batchSize > 0) { "'batchSize' must be positive. Please, check the configuration. $batchSize" }
        require(maxInactivityPeriod > 0) { "'maxInactivityPeriod' must be positive. Please, check the configuration. $maxInactivityPeriod" }
        require(timeSpan > 0) { "'timeSpan' must be positive. Please, check the configuration. $timeSpan" }
        require(eventBatchMaxBytes > 0) { "'eventBatchMaxBytes' must be positive. Please, check the configuration. $eventBatchMaxBytes" }
        require(eventBatchMaxEvents > 0) { "'eventBatchMaxEvents' must be positive. Please, check the configuration. $eventBatchMaxEvents" }
        require(eventBatchTimeSpan > 0) { "'eventBatchTimeSpan' must be positive. Please, check the configuration. $eventBatchTimeSpan" }
        require(kafkaBatchSize > 0) { "'kafkaBatchSize' must be positive. Please, check the configuration. $kafkaBatchSize" }
        require(kafkaLingerMillis > 0) { "'kafkaLingerMillis' must be positive. Please, check the configuration. $kafkaLingerMillis" }

        if (kafkaSecurityProtocol === null || kafkaSecurityProtocol == CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL) {
            val errMsg =" should be null if kafkaSecurityProtocol is null or '${CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL}'. Please, check the configuration."
            require(kafkaSaslKerberosServiceName === null) { "'kafkaSaslKerberosServiceName'$errMsg" }
            require(kafkaSaslMechanism === null) { "'kafkaSaslMechanism'$errMsg" }
            require(kafkaSaslJaasConfig === null) { "'kafkaSaslJaasConfig'$errMsg" }
        } else {
            val errMsg =" should not be null. Please, check the configuration."
            requireNotNull(kafkaSaslKerberosServiceName) { "'kafkaSaslKerberosServiceName'$errMsg" }
            requireNotNull(kafkaSaslMechanism) { "'kafkaSaslMechanism'$errMsg" }
            requireNotNull(kafkaSaslJaasConfig) { "'kafkaSaslJaasConfig'$errMsg" }
        }
    }

    @JsonIgnore
    val maxInactivityPeriodDuration: Duration = maxInactivityPeriodUnit.toMillis(maxInactivityPeriod).toDuration(DurationUnit.MILLISECONDS)

    @JsonIgnore
    val topicToBookAndAlias: Map<String, Pair<String?, String>> = topics.subscribe.asSequence()
        .filter { it.key == ANY_KEY }
        .map { it.topic to Pair(it.book, it.sessionAlias) }
        .toMap()

    @JsonIgnore
    val topicAndKeyToBookAndAlias: Map<KafkaStream, Pair<String?, String>> = topics.subscribe.asSequence()
        .filter { it.key != ANY_KEY }
        .map { KafkaStream(it.topic, it.key) to Pair(it.book, it.sessionAlias) }
        .toMap()

    /**
     * Matches th2 sessions with session groups
     * Key: book name and alias, value: session group
     */
    @JsonIgnore
    val bookAndAliasToGroup: Map<Pair<String?, String>, String?> = hashMapOf<Pair<String?, String>, String?>()
        .apply {
            (topics.subscribe.asSequence() + topics.publish.asSequence())
                .map { Pair(it.book, it.sessionAlias) to (it.sessionGroup ?: defaultSessionGroup ?: it.sessionAlias) }
                .forEach { (key, value) ->
                    val prevValue = put(key, value)
                    if (prevValue != null) {
                        require(value == prevValue) { "Different groups for '${key.second} alias in '${key.first}' book" }
                    }
                }
        }.withDefault { (_, alias) -> defaultSessionGroup ?: alias }

    val books: Set<String> = (topics.subscribe.asSequence() + topics.publish.asSequence())
        .mapNotNull { it.book }
        .toSet()

    /**
     * Matches th2 sessions with Kafka topics
     */
    @JsonIgnore
    val bookAndAliasToTopic: Map<Pair<String?, String>, KafkaStream> = topics.publish.asSequence()
        .map { Pair(it.book, it.sessionAlias) to KafkaStream(it.topic, it.key?.ifEmpty { null }) }
        .toMap()

    private fun checkForDuplicatedAliases(topics: List<TopicConfig>) {
        val seenAlias = hashSetOf<Pair<String?, String>>()

        for (topic in topics) {
            require(seenAlias.add(topic.book to topic.sessionAlias)) { "Duplicated alias: '${topic.sessionAlias}' in '${topic.book}' book" }
        }
    }

    private fun checkForDuplicatedTopics(topics: List<TopicConfig>) {
        val seenTopic = hashSetOf<String>()
        val seenStream = hashSetOf<Pair<String, String?>>()

        for (topic in topics) {
            require(!seenTopic.contains(topic.topic)) { "Duplicated topic: '${topic.topic}'" }

            if (topic.key == ANY_KEY) {
                seenTopic.add(topic.topic)
            }

            require(seenStream.add(topic.topic to topic.key)) { "Duplicated stream: topic: '${topic.topic}', key:'${topic.key}'" }
        }
    }

}

data class KafkaStream(
    @JsonProperty(required = true) val topic: String,
    @JsonProperty(required = true) val key: String?
)

class Topics(
    val publish: List<TopicConfig> = emptyList(),
    val subscribe: List<TopicConfig> = emptyList()
) {
    init {
        require(publish.isNotEmpty() || subscribe.isNotEmpty()) { "'topics.publish' and 'topics.subscribe' can't both be empty" }
    }
}

private const val ANY_KEY: String = ""
class TopicConfig(
    val topic: String,
    val key: String? = ANY_KEY,
    val sessionAlias: String,
    val sessionGroup: String? = null,
    val book: String? = null
)

enum class ResetOffset {
    NONE,
    BEGIN,
    END,
    TIME,
    MESSAGE
}