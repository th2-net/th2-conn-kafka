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

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.sequence
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.flowables.GroupedFlowable
import io.reactivex.rxjava3.plugins.RxJavaPlugins
import io.reactivex.rxjava3.processors.FlowableProcessor
import io.reactivex.rxjava3.processors.UnicastProcessor
import io.reactivex.rxjava3.subscribers.DisposableSubscriber
import mu.KotlinLogging
import java.io.Closeable
import java.util.function.Supplier

class MessageProcessor(
    rawSubscriberFactory: Supplier<MessageRouterSubscriber<RawMessageBatch>>,
    config: Config
) : Closeable {
    private val processor: FlowableProcessor<RawMessage> = UnicastProcessor.create()

    init {
        createPipeline().subscribe(TerminationSubscriber(rawSubscriberFactory, config))
    }

    fun process(message: RawMessage) = processor.onNext(message)

    override fun close() {
        LOGGER.info { "Shutdown pipeline scheduler" }
        MESSAGE_PROCESSOR_SCHEDULER.shutdown()
        LOGGER.info { "Complete pipeline publisher" }
        processor.onComplete()
    }

    private fun createPipeline(): Flowable<GroupedFlowable<String, RawMessage>> =
        processor.observeOn(MESSAGE_PROCESSOR_SCHEDULER).groupBy { it.metadata.id.connectionId.sessionGroup }

    private class TerminationSubscriber(
        private val rawSubscriberFactory: Supplier<MessageRouterSubscriber<RawMessageBatch>>,
        private val settings: Config
    ) : DisposableSubscriber<Flowable<RawMessage>>() {

        override fun onStart() {
            super.onStart()
            LOGGER.info { "Subscribed to pipeline" }
        }

        override fun onNext(flowable: Flowable<RawMessage>) {
            val messageConnectable = flowable.publish()
            createPackAndPublishPipeline(messageConnectable, rawSubscriberFactory, settings)
            messageConnectable.connect()
        }

        override fun onError(throwable: Throwable) = LOGGER.error(throwable) { "Upstream threw error" }
        override fun onComplete() = LOGGER.info { "Upstream is completed" }

        private fun createPackAndPublishPipeline(
            messageConnectable: Flowable<RawMessage>,
            rawSubscriberFactory: Supplier<MessageRouterSubscriber<RawMessageBatch>>,
            settings: Config
        ) {
            messageConnectable
                .doOnNext { LOGGER.trace { "Message before window with sequence ${it.sequence} and direction ${it.direction}" } }
                .window(settings.timeSpan, settings.timeSpanUnit, MESSAGE_PROCESSOR_SCHEDULER, settings.batchSize)
                .concatMapSingle { it.toList() }
                .filter { it.isNotEmpty() }
                .map { RawMessageBatch.newBuilder().addAllMessages(it).build() }
                .publish()
                .apply {
                    subscribe(rawSubscriberFactory.get())
                    connect()
                }
            LOGGER.info { "Connected to publish batches group" }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
        private val MESSAGE_PROCESSOR_SCHEDULER = RxJavaPlugins.createSingleScheduler(
            ThreadFactoryBuilder().setNameFormat("MessageProcessor-%d").build()
        )
    }
}