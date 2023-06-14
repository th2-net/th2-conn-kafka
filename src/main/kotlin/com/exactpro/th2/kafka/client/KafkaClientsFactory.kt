/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

class KafkaClientsFactory(private val config: Config) {
    fun getKafkaConsumer(): Consumer<String, ByteArray> = KafkaConsumer(
        Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId)
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java)
            put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, config.reconnectBackoffMs)
            put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, config.reconnectBackoffMaxMs)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.kafkaAutoOffsetReset)
            addSecuritySettings()
        }
    )

    fun getKafkaProducer(): Producer<String, ByteArray> = KafkaProducer(
        Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java)
            put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, config.reconnectBackoffMs)
            put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, config.reconnectBackoffMaxMs)
            put(ProducerConfig.BATCH_SIZE_CONFIG, config.kafkaBatchSize)
            put(ProducerConfig.LINGER_MS_CONFIG, config.kafkaLingerMillis)
            addSecuritySettings()
        }
    )

    private fun Properties.addSecuritySettings() {
        val securityProtocol = config.kafkaSecurityProtocol
        if (securityProtocol !== null && securityProtocol != CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL) {
            put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol)
            put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, config.kafkaSaslKerberosServiceName)
            put(SaslConfigs.SASL_MECHANISM, config.kafkaSaslMechanism)
            put(SaslConfigs.SASL_JAAS_CONFIG, config.kafkaSaslJaasConfig)
        }

        if (config.kafkaSecurityTruststoreLocation !== null) {
            put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, config.kafkaSecurityTruststoreLocation)
            if (config.kafkaSecurityTruststorePassword !== null) {
                put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, config.kafkaSecurityTruststorePassword)
            }
        }
    }
}