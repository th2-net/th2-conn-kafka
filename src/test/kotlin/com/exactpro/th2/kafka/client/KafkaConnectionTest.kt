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

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.bookName
import com.exactpro.th2.common.utils.message.direction
import com.exactpro.th2.common.utils.message.sessionAlias
import com.exactpro.th2.common.utils.message.sessionGroup
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration
import com.exactpro.th2.common.schema.factory.CommonFactory
import java.time.Duration
import com.google.protobuf.UnsafeByteOperations
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import kotlin.test.Test
import org.mockito.kotlin.*
import java.lang.IllegalStateException
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class KafkaConnectionTest {
    private val commonFactory: CommonFactory = mock {
        on { boxConfiguration } doReturn BoxConfiguration().apply { bookName = "book_01" }
        on { newMessageIDBuilder() } doReturn MessageID.newBuilder().setBookName("book_01")
    }

    private val testMessageText = "QWERTY"
    private val messageProcessor: RawMessageProcessor = mock()
    private val eventSender: EventSender = mock()

    private val consumerRecords: ConsumerRecords<String, ByteArray> = mock {
        on { isEmpty } doReturn false
        on { iterator() } doReturn mutableListOf(
            ConsumerRecord(
                "topic_03",
                0,
                0,
                Instant.now().toEpochMilli(),
                null,
                0,
                0,
                "key_03",
                testMessageText.toByteArray(),
                mock(),
                null
            )
        ).iterator()
    }

    private val kafkaConsumer: Consumer<String, ByteArray> = mock {
        on { poll(any<Duration>()) } doReturn consumerRecords
    }
    private val kafkaProducer: Producer<String, ByteArray> = mock()

    private val kafkaClientsFactory: KafkaClientsFactory = mock {
        on { getKafkaConsumer() } doReturn kafkaConsumer
        on { getKafkaProducer() } doReturn kafkaProducer
    }

    private val connection = KafkaConnection(
        Config(
            aliasToTopic = mapOf("alias_01" to KafkaTopic("topic_01"), "alias_02" to KafkaTopic("topic_02")),
            aliasToTopicAndKey = mapOf("alias_03" to KafkaStream("topic_03", "key_03", true)),
            sessionGroups = mapOf("group_01" to listOf("alias_01")),
            messagePublishingEvents = true
        ),
        commonFactory,
        messageProcessor,
        eventSender,
        kafkaClientsFactory
    )

    @Test
    fun `publish message`() {
        val testMessage = RawMessage.newBuilder()
            .setBody(UnsafeByteOperations.unsafeWrap(testMessageText.toByteArray())).apply {
                sessionAlias = "alias_01"
            }
            .build()

        connection.publish(testMessage)

        val messageBuilderCaptor = argumentCaptor<RawMessage.Builder>()
        val processorCallbackCaptor = argumentCaptor<(RawMessage) -> Unit>()
        verify(messageProcessor, only()).onMessage(messageBuilderCaptor.capture(), processorCallbackCaptor.capture())

        val outMessage = messageBuilderCaptor.firstValue.build()
        assertThat(outMessage.bookName).isEqualTo("book_01")
        assertThat(outMessage.sessionAlias).isEqualTo("alias_01")
        assertThat(outMessage.sessionGroup).isEqualTo("group_01")
        assertThat(outMessage.direction).isEqualTo(Direction.SECOND)
        assertThat(outMessage.body.toStringUtf8()).isEqualTo(testMessageText)

        processorCallbackCaptor.firstValue.invoke(outMessage) // complete CompletableFuture

        val recordCaptor = argumentCaptor<ProducerRecord<String, ByteArray>>()
        val producerCallbackCaptor = argumentCaptor<Callback>()
        verify(kafkaProducer, only()).send(recordCaptor.capture(), producerCallbackCaptor.capture())

        producerCallbackCaptor.firstValue.onCompletion(null, null)
        val kafkaRecord = recordCaptor.firstValue
        assertThat(kafkaRecord.topic()).isEqualTo("topic_01")
        assertThat(kafkaRecord.key()).isNull()
        assertThat(String(kafkaRecord.value())).isEqualTo(testMessageText)
        verify(eventSender, only()).onEvent(any(), eq("Send message"), eq(outMessage), eq(null), eq(null), eq(null))
    }

    @Test
    fun `alias not found`() {
        val testMessage = RawMessage.newBuilder()
            .setBody(UnsafeByteOperations.unsafeWrap("QWERTY".toByteArray())).apply {
                sessionAlias = "wrong_alias"
            }
            .build()

        assertThatThrownBy { connection.publish(testMessage) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("alias")
    }

    @Test
    fun `poll messages from kafka`() {
        val executor = Executors.newSingleThreadExecutor()
        executor.execute(connection)
        Thread.sleep(100)
        executor.shutdownNow()
        executor.awaitTermination(1000, TimeUnit.SECONDS)

        val messageBuilderCaptor = argumentCaptor<RawMessage.Builder>()
        verify(messageProcessor, only()).onMessage(messageBuilderCaptor.capture(), any())

        val messageBuilder = messageBuilderCaptor.firstValue
        assertThat(messageBuilder.bookName).isEqualTo("book_01")
        assertThat(messageBuilder.sessionAlias).isEqualTo("alias_03")
        assertThat(messageBuilder.sessionGroup).isEqualTo("alias_03") // if no group name provided sessionAlias is used as group name
        assertThat(messageBuilder.direction).isEqualTo(Direction.FIRST)
        assertThat(messageBuilder.body.toStringUtf8()).isEqualTo(testMessageText)
    }
}