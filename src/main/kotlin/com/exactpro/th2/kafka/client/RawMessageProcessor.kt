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
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.message.bookName
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.common.utils.message.logId
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.common.utils.message.direction
import com.exactpro.th2.common.utils.message.sessionGroup
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.CompletableFuture
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock

class RawMessageProcessor(
    private val maxBatchSize: Int,
    private val maxFlushTime: Long,
    private val maxFlushTimeUnit: TimeUnit,
    private val onBatch: (RawMessageBatch) -> Unit
) : AutoCloseable {
    private val messageQueue: BlockingQueue<Pair<RawMessage.Builder, (RawMessage) -> Unit>> = LinkedBlockingQueue()
    private val batchQueue: BlockingQueue<RawMessageBatch> = LinkedBlockingQueue()
    private val batchFlusherExecutor = Executors.newSingleThreadScheduledExecutor()

    private val messageReceiverThread = thread(name = "message-receiver") {
        val counters: MutableMap<Triple<String, String, Direction>, () -> Long> = HashMap()
        val builders: MutableMap<Pair<String, String>, BatchHolder> = HashMap()

        while (true) {
            val messageAndCallback = messageQueue.take()
            if (messageAndCallback === TERMINAL_MESSAGE) break
            val (messageBuilder, onMessageBuilt) = messageAndCallback

            messageBuilder.metadataBuilder.idBuilder.apply {
                timestamp = Instant.now().toTimestamp()
                sequence = when (messageBuilder.direction) {
                    Direction.FIRST, Direction.SECOND -> {
                        val counter = counters.getOrPut(Triple(bookName, messageBuilder.sessionAlias, messageBuilder.direction)) {
                            createSequence()
                        }
                        counter()
                    }
                    else -> error("Unrecognized direction")
                }
            }

            check(messageBuilder.bookName.isNotEmpty()) { "bookName should be assigned to all messages" }
            val sessionGroup: String = checkNotNull(messageBuilder.sessionGroup) { "sessionGroup should be assigned to all messages" }
            val message = messageBuilder.build()
            onMessageBuilt(message)
            builders.getOrPut(messageBuilder.bookName to sessionGroup, ::BatchHolder).addMessage(message)
        }

        builders.values.forEach(BatchHolder::enqueueBatch)
        batchFlusherExecutor.shutdown()
        if (!batchFlusherExecutor.awaitTermination(TERMINATION_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            LOGGER.warn { "batchFlusherExecutor did not terminate" }
        }
        batchQueue.add(TERMINAL_BATCH)
    }

    inner class BatchHolder {
        private val batchBuilder: RawMessageBatch.Builder = RawMessageBatch.newBuilder()
        private var flusherFuture: Future<*> = CompletableFuture.completedFuture(null)
        private val lock: Lock = ReentrantLock()

        fun addMessage(message: RawMessage) = lock.withLock {
            batchBuilder.addMessages(message)
            LOGGER.trace { "Message ${message.id.logId} added to batch." }
            when (batchBuilder.messagesCount) {
                maxBatchSize -> enqueueBatch()
                1 -> flusherFuture = batchFlusherExecutor.schedule(::enqueueBatch, maxFlushTime, maxFlushTimeUnit)
            }
        }

        fun enqueueBatch() = lock.withLock {
            if (batchBuilder.messagesCount > 0) {
                flusherFuture.cancel(false)
                batchQueue.add(batchBuilder.build())
                batchBuilder.clear()
            }
        }
    }

    private val batchSenderThread = thread(name = "batch-sender") {
        while (true) {
            val batch = batchQueue.take()
            if (batch === TERMINAL_BATCH) break
            onBatch(batch)
        }
    }

    fun onMessage(messageBuilder: RawMessage.Builder, onMessageBuilt: (RawMessage) -> Unit = EMPTY_CALLBACK) {
        messageQueue.add(messageBuilder to onMessageBuilt)
    }

    override fun close() {
        messageQueue.add(TERMINAL_MESSAGE)
        messageReceiverThread.awaitTermination(TERMINATION_WAIT_TIMEOUT_MS)
        batchSenderThread.awaitTermination(TERMINATION_WAIT_TIMEOUT_MS)
    }

    private fun Thread.awaitTermination(timeout: Long) {
        join(timeout)
        if (isAlive) {
            LOGGER.warn { "Thread '$name' did not terminate." }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
        private val EMPTY_CALLBACK: (RawMessage) -> Unit = {}
        private val TERMINAL_MESSAGE: Pair<RawMessage.Builder, (RawMessage) -> Unit> = RawMessage.newBuilder() to EMPTY_CALLBACK
        private val TERMINAL_BATCH = RawMessageBatch.newBuilder().build()
        private const val TERMINATION_WAIT_TIMEOUT_MS = 5_000L

        private val Instant.epochNanos
            get() = TimeUnit.SECONDS.toNanos(epochSecond) + nano

        private fun createSequence(): () -> Long = AtomicLong(Instant.now().epochNanos)::incrementAndGet // TODO: we don't need atomicity here
    }
}