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

@file:JvmName("Main")

package com.exactpro.th2.kafka.client

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.message.bookName
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.common.utils.message.logId
import com.exactpro.th2.kafka.client.utility.storeEvent
import mu.KotlinLogging
import java.util.Deque
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.concurrent.thread
import kotlin.system.exitProcess

private val LOGGER = KotlinLogging.logger {}
private const val INPUT_QUEUE_ATTRIBUTE = "send"

fun main(args: Array<String>) {
    val resources: Deque<Pair<String, () -> Unit>> = ConcurrentLinkedDeque()
    val shutdownLatch = CountDownLatch(1)
    Runtime.getRuntime().addShutdownHook(thread(start = false, name = "shutdown-hook") {
        resources.descendingIterator().forEach { (resource, destructor) ->
            LOGGER.debug { "Destroying resource: $resource" }
            runCatching(destructor)
                .onSuccess { LOGGER.debug { "Successfully destroyed resource: $resource" } }
                .onFailure { LOGGER.error(it) { "Failed to destroy resource: $resource" } }
        }
        shutdownLatch.countDown()
    })

    val factory = runCatching { CommonFactory.createFromArguments(*args) }.getOrElse {
        LOGGER.error(it) { "Failed to create common factory with arguments: ${args.joinToString(" ")}" }
        CommonFactory()
    }.apply { resources += "factory" to ::close }

    runCatching {
        val config: Config = factory.getCustomConfiguration(Config::class.java)
        val messageRouterRawBatch = factory.messageRouterRawBatch

        val messageProcessor = RawMessageProcessor(config.batchSize, config.timeSpan, config.timeSpanUnit) {
            LOGGER.trace { "Sending batch with ${it.messagesCount} messages to MQ." }
            it.runCatching(messageRouterRawBatch::send)
                .onFailure { e -> LOGGER.error(e) {
                    it.messagesOrBuilderList
                    "Could not send message batch to MQ: ${it.toJson()}" }
                }
        }.apply { resources += "message processor" to ::close }

        val eventSender = EventSender(factory.eventBatchRouter, factory.rootEventId)

        if (config.createTopics) KafkaConnection.createTopics(config)
        val connection = KafkaConnection(config, factory, messageProcessor, eventSender, KafkaClientsFactory(config))
            .apply { resources += "kafka connection" to ::close }

        Executors.newSingleThreadExecutor().apply {
            resources += "executor service" to { this.shutdownNow() }
            execute(connection)
        }

        val mqListener: (DeliveryMetadata, RawMessageBatch) -> Unit = { metadata, batch ->
            LOGGER.trace { "Batch with ${batch.messagesCount} messages received from MQ"}
            for (message in batch.messagesList) {
                LOGGER.trace { "Message ${message.id.logId} extracted from batch." }

                val bookName = message.bookName
                if (bookName.isNotEmpty() && bookName != factory.boxConfiguration.bookName) {
                    val errorText = "Expected bookName: '${factory.boxConfiguration.bookName}', actual '$bookName' in message ${message.id.logId}"
                    LOGGER.error { errorText }
                    eventSender.onEvent(errorText, "Error", status = Event.Status.FAILED)
                    continue
                }

                runCatching {
                    connection.publish(message)
                }.onFailure {
                    val errorText = "Could not publish message ${message.id.logId}. Consumer tag ${metadata.consumerTag}"
                    LOGGER.error(it) { errorText }
                    eventSender.onEvent(errorText, "SendError", message, it)
                }
            }
        }

        runCatching {
            messageRouterRawBatch.subscribeAll(mqListener, INPUT_QUEUE_ATTRIBUTE)
        }.onSuccess {
            resources += "queue listener" to it::unsubscribe
        }.onFailure {
            throw IllegalStateException("Failed to subscribe to input queue", it)
        }
    }.onFailure {
        LOGGER.error(it) { "Error during working with Kafka connection. Exiting the program" }
        exitProcess(2)
    }

    LOGGER.info { "Successfully started." }
    shutdownLatch.await()
    LOGGER.info { "Microservice shutted down." }
}

class EventSender(private val eventRouter: MessageRouter<EventBatch>, private val rootEventId: EventID) {
    fun onEvent(
        name: String,
        type: String,
        message: RawMessage? = null,
        exception: Throwable? = null,
        status: Event.Status? = null,
        parentEventId: EventID? = null
    ) {
        val event = Event
            .start()
            .endTimestamp()
            .name(name)
            .type(type)

        if (message != null) {
            event.messageID(message.id)
        }

        if (exception != null) {
            event.exception(exception, true).status(Event.Status.FAILED)
        }

        if (status != null) {
            event.status(status)
        }

        eventRouter.storeEvent(event, parentEventId ?: rootEventId)
    }
}