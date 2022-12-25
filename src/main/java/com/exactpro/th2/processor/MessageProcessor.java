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

package com.exactpro.th2.processor;

import static io.reactivex.rxjava3.plugins.RxJavaPlugins.createSingleScheduler;

import java.io.Closeable;
import java.util.Objects;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.KafkaMain.MessageRouterSubscriber;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.configuration.KafkaConfigurationSettings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.flowables.ConnectableFlowable;
import io.reactivex.rxjava3.flowables.GroupedFlowable;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import io.reactivex.rxjava3.subscribers.DisposableSubscriber;

public class MessageProcessor implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessor.class);
    private static final Scheduler MESSAGE_PROCESSOR_SCHEDULER = createSingleScheduler(new ThreadFactoryBuilder()
            .setNameFormat("MessageProcessor-%d").build());

    private final FlowableProcessor<RawMessage> processor = UnicastProcessor.create();

    public MessageProcessor(Supplier<MessageRouterSubscriber<RawMessageBatch>> rawSubscriberFactory,
            KafkaConfigurationSettings settings) {
        createPipeline().subscribe(new TerminationSubscriber(rawSubscriberFactory, settings));
    }

    public void process(RawMessage message) {
        processor.onNext(message);
    }

    @Override
    public void close() {
        LOGGER.info("Shutdown pipeline scheduler");
        MESSAGE_PROCESSOR_SCHEDULER.shutdown();
        LOGGER.info("Complete pipeline publisher");
        processor.onComplete();
    }

    private @NonNull Flowable<GroupedFlowable<String, RawMessage>> createPipeline() {
        return processor.observeOn(MESSAGE_PROCESSOR_SCHEDULER)
                .groupBy(msg -> msg.getMetadata().getId().getConnectionId().getSessionGroup());
    }

    @SuppressWarnings("ParameterNameDiffersFromOverriddenParameter")
    private static class TerminationSubscriber extends DisposableSubscriber<Flowable<RawMessage>> {

        private final Supplier<MessageRouterSubscriber<RawMessageBatch>> rawSubscriberFactory;
        private final KafkaConfigurationSettings settings;
        public TerminationSubscriber(Supplier<MessageRouterSubscriber<RawMessageBatch>> rawSubscriberFactory,
                KafkaConfigurationSettings settings) {
            this.rawSubscriberFactory = Objects.requireNonNull(rawSubscriberFactory, "SubscriberFactory cannot be null");
            this.settings = Objects.requireNonNull(settings, "Kafca connection settings cannot be null");
        }

        @Override
        protected void onStart() {
            super.onStart();
            LOGGER.info("Subscribed to pipeline");
        }

        @Override
        public void onError(Throwable throwable) {
            LOGGER.error("Upstream threw error", throwable);
        }

        @Override
        public void onComplete() {
            LOGGER.info("Upstream is completed");
        }

        @Override
        public void onNext(Flowable<RawMessage> flowable) {
            ConnectableFlowable<RawMessage> messageConnectable = flowable.publish();
            createPackAndPublishPipeline(messageConnectable, rawSubscriberFactory, settings);
            messageConnectable.connect();
        }

        private static void createPackAndPublishPipeline(Flowable<RawMessage> messageConnectable,
                Supplier<MessageRouterSubscriber<RawMessageBatch>> rawSubscriberFactory, KafkaConfigurationSettings settings) {
            ConnectableFlowable<RawMessageBatch> batchConnectable = messageConnectable
                    .window(settings.getTimespan(), settings.getTimespanUnit(), MESSAGE_PROCESSOR_SCHEDULER, settings.getBatchSize())
                    .concatMapSingle(Flowable::toList)
                    .filter(list -> !list.isEmpty())
                    .map(list -> RawMessageBatch.newBuilder()
                            .addAllMessages(list)
                            .build())
                    .publish();
            batchConnectable.subscribe(rawSubscriberFactory.get());
            batchConnectable.connect();
            LOGGER.info("Connected to publish batches group");
        }
    }
}