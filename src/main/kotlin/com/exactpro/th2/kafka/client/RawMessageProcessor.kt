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

import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage
import com.exactpro.th2.common.grpc.Direction as ProtoDirection
import com.exactpro.th2.common.grpc.RawMessageBatch as ProtoRawMessageBatch
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.*
import com.exactpro.th2.common.utils.message.direction
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.common.utils.message.logId
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.common.utils.message.transport.logId
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

interface MessageAcceptor<MESSAGE, MESSAGE_BUILDER> {
    fun onMessage(messageBuilder: MESSAGE_BUILDER, onMessageBuilt: (MESSAGE, String) -> Unit = { _, _ -> })
}

abstract class RawMessageProcessor<BATCH, BATCH_BUILDER, MESSAGE, MESSAGE_BUILDER>(
    private val maxBatchSize: Int,
    private val maxFlushTime: Long,
    private val maxFlushTimeUnit: TimeUnit,
    protected val book: String,
    private val aliasToSessionGroup: Map<String, String>,
    private val onBatch: (BATCH) -> Unit
) : MessageAcceptor<MESSAGE, MESSAGE_BUILDER>, AutoCloseable {
    protected val emptyCallback: (MESSAGE, String) -> Unit = { _, _ -> }
    protected abstract val terminalMessage: Pair<MESSAGE_BUILDER, (MESSAGE, String) -> Unit>
    protected abstract val terminalBatch: BATCH

    protected abstract fun newBatchBuilder(sessionGroup: String): BATCH_BUILDER
    protected abstract fun BATCH_BUILDER.addMessage(message: MESSAGE): BATCH_BUILDER
    protected abstract val BATCH_BUILDER.size: Int
    protected abstract fun BATCH_BUILDER.buildBatch(): BATCH
    protected abstract fun BATCH_BUILDER.clearBatch()
    protected abstract val MESSAGE.logId: String
    protected abstract val MESSAGE_BUILDER.builderSessionAlias: String
    protected abstract fun MESSAGE_BUILDER.completeBuilding(counters: MutableMap<Pair<String, Direction>, () -> Long>, sessionGroup: String): MESSAGE

    private val messageQueue: BlockingQueue<Pair<MESSAGE_BUILDER, (MESSAGE, String) -> Unit>> = LinkedBlockingQueue()
    private val batchQueue: BlockingQueue<BATCH> = LinkedBlockingQueue()
    private val batchFlusherExecutor = Executors.newSingleThreadScheduledExecutor()

    private val messageReceiverThread = thread(name = "message-receiver") {
        val counters: MutableMap<Pair<String, Direction>, () -> Long> = hashMapOf()
        val builders: MutableMap<String, BatchHolder> = hashMapOf()

        while (true) {
            val messageAndCallback = messageQueue.take()
            if (messageAndCallback === terminalMessage) break
            val (messageBuilder, onMessageBuilt) = messageAndCallback

            val sessionGroup: String = aliasToSessionGroup.getValue(checkNotNull(messageBuilder.builderSessionAlias) { "sessionAlias should be assigned to all messages" })

            val message = messageBuilder.completeBuilding(counters, sessionGroup)

            onMessageBuilt(message, sessionGroup)
            builders.getOrPut(sessionGroup) { BatchHolder(sessionGroup) }.addMessage(message)
        }

        builders.values.forEach { it.enqueueBatch() }
        batchFlusherExecutor.shutdown()
        if (!batchFlusherExecutor.awaitTermination(TERMINATION_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            LOGGER.warn { "batchFlusherExecutor did not terminate" }
        }
        batchQueue.add(terminalBatch)
    }

    inner class BatchHolder(
        sessionGroup: String
    ) {
        private val batchBuilder: BATCH_BUILDER = newBatchBuilder(sessionGroup)
        private var flusherFuture: Future<*> = CompletableFuture.completedFuture(null)
        private val lock: Lock = ReentrantLock()

        fun addMessage(message: MESSAGE) = lock.withLock {
            batchBuilder.addMessage(message)
            LOGGER.trace { "Message ${message.logId} added to batch." }
            when (batchBuilder.size) {
                maxBatchSize -> enqueueBatch()
                1 -> flusherFuture = batchFlusherExecutor.schedule(::enqueueBatch, maxFlushTime, maxFlushTimeUnit)
            }
        }

        fun enqueueBatch() = lock.withLock {
            if (batchBuilder.size > 0) {
                flusherFuture.cancel(false)
                batchQueue.add(batchBuilder.buildBatch())
                batchBuilder.clearBatch()
            }
        }
    }

    private val batchSenderThread = thread(name = "batch-sender") {
        while (true) {
            val batch = batchQueue.take()
            if (batch === terminalBatch) break
            onBatch(batch)
        }
    }

    override fun onMessage(messageBuilder: MESSAGE_BUILDER, onMessageBuilt: (MESSAGE, String) -> Unit) {
        messageQueue.add(messageBuilder to onMessageBuilt)
    }

    override fun close() {
        messageQueue.add(terminalMessage)
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
        private const val TERMINATION_WAIT_TIMEOUT_MS = 5_000L
        @JvmStatic
        protected fun createSequence(): () -> Long = AtomicLong(Instant.now().epochNanos)::incrementAndGet // TODO: we don't need atomicity here

        private val Instant.epochNanos
            get() = TimeUnit.SECONDS.toNanos(epochSecond) + nano
    }
}

class ProtoRawMessageProcessor(
    maxBatchSize: Int,
    maxFlushTime: Long,
    maxFlushTimeUnit: TimeUnit,
    book: String,
    aliasToSessionGroup: Map<String, String>,
    onBatch: (ProtoRawMessageBatch) -> Unit
) : RawMessageProcessor<ProtoRawMessageBatch, ProtoRawMessageBatch.Builder, ProtoRawMessage, ProtoRawMessage.Builder>(
    maxBatchSize,
    maxFlushTime,
    maxFlushTimeUnit,
    book,
    aliasToSessionGroup,
    onBatch
) {
    override val terminalMessage: Pair<ProtoRawMessage.Builder, (ProtoRawMessage, String) -> Unit> = ProtoRawMessage.newBuilder() to emptyCallback
    override val terminalBatch: ProtoRawMessageBatch = ProtoRawMessageBatch.newBuilder().build()
    override fun newBatchBuilder(sessionGroup: String): ProtoRawMessageBatch.Builder = ProtoRawMessageBatch.newBuilder()
    override val ProtoRawMessageBatch.Builder.size: Int get() = messagesCount
    override fun ProtoRawMessageBatch.Builder.buildBatch(): ProtoRawMessageBatch = build()
    override val ProtoRawMessage.logId: String get() = id.logId
    override val ProtoRawMessage.Builder.builderSessionAlias: String get() = id.connectionId.sessionAlias
    override fun ProtoRawMessageBatch.Builder.addMessage(message: ProtoRawMessage): RawMessageBatch.Builder = addMessages(message)

    override fun ProtoRawMessageBatch.Builder.clearBatch() {
        this.clear()
    }

    override fun ProtoRawMessage.Builder.completeBuilding(counters: MutableMap<Pair<String, Direction>, () -> Long>, sessionGroup: String): ProtoRawMessage{
        val messageBuilder = this
        metadataBuilder.idBuilder.apply {
            bookName = book
            connectionIdBuilder.sessionGroup = sessionGroup
            timestamp = Instant.now().toTimestamp()
            sequence = when (this.direction) {
                ProtoDirection.FIRST, ProtoDirection.SECOND -> {
                    val counter = counters.getOrPut(messageBuilder.sessionAlias to messageBuilder.direction.transport) { createSequence() }
                    counter()
                }
                else -> error("Unrecognized direction")
            }
        }
        return build()
    }
}

class TransportRawMessageProcessor(
    maxBatchSize: Int,
    maxFlushTime: Long,
    maxFlushTimeUnit: TimeUnit,
    book: String,
    aliasToSessionGroup: Map<String, String>,
    onBatch: (GroupBatch) -> Unit
) : RawMessageProcessor<GroupBatch, GroupBatch.Builder, RawMessage, RawMessage.Builder>(
    maxBatchSize,
    maxFlushTime,
    maxFlushTimeUnit,
    book,
    aliasToSessionGroup,
    onBatch
) {
    override val terminalMessage: Pair<RawMessage.Builder, (RawMessage, String) -> Unit> = RawMessage.builder() to { _, _ -> }
    override val terminalBatch: GroupBatch = GroupBatch("", "")

   override fun newBatchBuilder(sessionGroup: String): GroupBatch.Builder = GroupBatch.builder()
       .setBook(book)
       .setSessionGroup(sessionGroup)

    override fun GroupBatch.Builder.addMessage(message: RawMessage) = addGroup(MessageGroup(listOf(message)))
    override val GroupBatch.Builder.size: Int get() = this.groupsBuilder().size
    override fun GroupBatch.Builder.buildBatch(): GroupBatch = build()

    override fun GroupBatch.Builder.clearBatch() {
        this.setGroups(mutableListOf())
    }

    override val RawMessage.logId: String get() = id.logId
    override val RawMessage.Builder.builderSessionAlias: String get() = this.idBuilder().sessionAlias

    override fun RawMessage.Builder.completeBuilding(
        counters: MutableMap<Pair<String, Direction>, () -> Long>,
        sessionGroup: String
    ): RawMessage {
        val idBuilder = idBuilder()

        val sequence = when (idBuilder.direction) {
            Direction.INCOMING, Direction.OUTGOING -> {
                val counter = counters.getOrPut(idBuilder.sessionAlias to idBuilder.direction) {
                    createSequence()
                }
                counter()
            }
            else -> error("Unrecognized direction")
        }

        idBuilder.setTimestamp(Instant.now())
            .setSequence(sequence)

        return build()
    }
}