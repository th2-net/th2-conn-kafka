/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2;

import java.io.Closeable;
import java.io.IOException;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.configuration.KafkaConfigurationSettings;
import com.exactpro.th2.processor.MessageProcessor;
import com.google.protobuf.MessageOrBuilder;

import io.reactivex.rxjava3.subscribers.DisposableSubscriber;

public class KafkaMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMain.class);

    public static void main(String[] args) {
        Disposer disposer = new Disposer();
        Runtime.getRuntime().addShutdownHook(new Thread(disposer::dispose, "Shutdown hook"));
        CommonFactory factory;
        try {
            factory = CommonFactory.createFromArguments(args);
        } catch (RuntimeException e) {
            factory = new CommonFactory();
            LOGGER.warn("Can not create common factory from arguments", e);
        }

        CommonFactory finalFactory = factory;
        disposer.register(() -> {
            LOGGER.info("Closing factory");
            finalFactory.close();
        });

        try {
            KafkaConfigurationSettings settings = factory.getCustomConfiguration(KafkaConfigurationSettings.class);
            validateSettings(settings);
            MessageRouter<RawMessageBatch> rawBatchRouter = factory.getMessageRouterRawBatch();

            MessageProcessor messageProcessor = new MessageProcessor(
                    () -> new MessageRouterSubscriber<>(rawBatchRouter), settings);
            disposer.register(() -> {
                LOGGER.info("Closing processor");
                messageProcessor.close();
            });
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            executorService.execute(new KafkaConnection(settings, messageProcessor));
            disposer.register(() -> {
                LOGGER.info("Closing executor service");
                executorService.shutdownNow();
            });
        } catch (RuntimeException ex) {
            LOGGER.error("Error during working with Kafka connection. Exiting the program", ex);
            System.exit(2);
        }
    }

    public static void validateSettings(KafkaConfigurationSettings settings) {
        if (settings.getTopicToSessionAliasMap().isEmpty()) {
            throw new IllegalArgumentException("No topics was provided. Please, check the configuration");
        }
        settings.getTopicToSessionAliasMap().forEach((topic, alias) -> {
            if(alias.isEmpty()) {
                throw new IllegalArgumentException("Session alias can't be empty. Please, check the configuration for " + topic);
            }
        });
        if (settings.getReconnectBackoffMaxMs() <= 0) {
            throw new IllegalArgumentException("ReconnectBackoffMaxMs must be positive. Please, check the configuration. " + settings.getReconnectBackoffMaxMs());
        }
        if (settings.getReconnectBackoffMs() <= 0) {
            throw new IllegalArgumentException("ReconnectBackoffMs must be positive. Please, check the configuration. " + settings.getReconnectBackoffMs());
        }
        if (settings.getBatchSize() <= 0) {
            throw new IllegalArgumentException("Batch size must be positive. Please, check the configuration. " + settings.getBatchSize());
        }
        if (settings.getAcceptableBreak() <= 0) {
            throw new IllegalArgumentException("AcceptableBreak must be positive. Please, check the configuration. " + settings.getAcceptableBreak());
        }
        if (settings.getTimespan() <= 0) {
            throw new IllegalArgumentException("Timespan must be positive. Please, check the configuration. " + settings.getTimespan());
        }

    }

    public static class MessageRouterSubscriber<T extends MessageOrBuilder> extends DisposableSubscriber<T> implements Closeable {
        private static final Logger LOGGER = LoggerFactory.getLogger(MessageRouterSubscriber.class);
        private final MessageRouter<T> messageRouter;

        public MessageRouterSubscriber(MessageRouter<T> messageRouter) {
            this.messageRouter = Objects.requireNonNull(messageRouter, "Message router cannot be null");
        }

        @Override
        public void onNext(T message) {
            try {
                messageRouter.send(message);
            } catch (IOException e) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Could not send message to mq: {}", MessageUtils.toJson(message), e);
                }

            }
        }

        @Override
        public void onError(Throwable t) {
            LOGGER.error("Could not process message", t);
        }

        @Override
        public void onComplete() {
            LOGGER.info("Upstream is completed");
        }

        @Override
        public void close() {
        }
    }

    public interface Disposable {
        void dispose() throws Exception;
    }

    public static class Disposer {

        private final Deque<Disposable> disposableQueue = new ConcurrentLinkedDeque<>();

        public void register(Disposable disposable) {
            disposableQueue.push(disposable);
        }

        /**
         * Disposes registered resources in LIFO order.
         */
        public void dispose() {
            LOGGER.info("Disposing ...");

            for (Disposable disposable : disposableQueue) {
                try {
                    disposable.dispose();
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }

            LOGGER.info("Disposed");
        }
    }
}
