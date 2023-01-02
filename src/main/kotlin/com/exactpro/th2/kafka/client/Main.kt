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

@file:JvmName("Main")

package com.exactpro.th2.kafka.client

import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter
import com.google.protobuf.MessageOrBuilder
import io.reactivex.rxjava3.subscribers.DisposableSubscriber
import mu.KotlinLogging
import java.io.Closeable
import java.util.Deque
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executors
import kotlin.concurrent.thread
import kotlin.system.exitProcess

private val LOGGER = KotlinLogging.logger {}

fun main(args: Array<String>) {
    val resources: Deque<Pair<String, () -> Unit>> = ConcurrentLinkedDeque()

    Runtime.getRuntime().addShutdownHook(thread(start = false, name = "shutdown-hook") {
        resources.descendingIterator().forEach { (resource, destructor) ->
            LOGGER.debug { "Destroying resource: $resource" }
            runCatching(destructor)
                .onSuccess { LOGGER.debug { "Successfully destroyed resource: $resource" } }
                .onFailure { LOGGER.error(it) { "Failed to destroy resource: $resource" } }
        }
    })

    val factory = args.runCatching { CommonFactory.createFromArguments(*args) }.getOrElse {
        LOGGER.error(it) { "Failed to create common factory with arguments: ${args.joinToString(" ")}" }
        CommonFactory()
    }.apply { resources += "factory" to ::close }

    runCatching {
        val config: Config = factory.getCustomConfiguration(Config::class.java)
        val messageProcessor = MessageProcessor({ MessageRouterSubscriber(factory.messageRouterRawBatch) }, config)
            .apply { resources += "processor" to ::close }
        Executors.newSingleThreadExecutor().apply {
            resources += "executor service" to { this.shutdownNow() }
            execute(KafkaConnection(factory, messageProcessor))
        }
    }.onFailure {
        LOGGER.error(it) { "Error during working with Kafka connection. Exiting the program" }
        exitProcess(2)
    }
}

class MessageRouterSubscriber<T : MessageOrBuilder>(private val messageRouter: MessageRouter<T>) :
    DisposableSubscriber<T>(), Closeable {

    override fun onNext(message: T) {
        message.runCatching(messageRouter::send)
            .onFailure { LOGGER.error(it) { "Could not send message to mq: ${message.toJson()}" } }
    }

    override fun onError(t: Throwable) = LOGGER.error(t) { "Could not process message" }
    override fun onComplete() = LOGGER.info { "Upstream is completed" }
    override fun close() {}

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}