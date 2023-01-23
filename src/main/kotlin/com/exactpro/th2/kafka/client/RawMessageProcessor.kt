package com.exactpro.th2.kafka.client

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.message.toTimestamp
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.CompletableFuture
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock

class RawMessageProcessor(
    private val maxBatchSize: Int,
    private val maxFlushTime: Long,
    private val maxFlushTimeUnit: TimeUnit,
    private val sequenceProducer: (RawMessage.Builder) -> Long,
    private val onBatch: (RawMessageBatch) -> Unit
) : AutoCloseable {
    private val messageQueue: BlockingQueue<RawMessage.Builder> = LinkedBlockingQueue()
    private val batchQueue: BlockingQueue<RawMessageBatch> = LinkedBlockingQueue()

    private val messageReceiverThread = thread(name = "message-receiver") {
        val batchFlusherExecutor = Executors.newSingleThreadScheduledExecutor()
        val batchBuilder = RawMessageBatch.newBuilder()
        var flusherFuture: Future<*> = CompletableFuture.completedFuture(null)
        val lock = ReentrantLock()

        fun enqueueBatch() = lock.withLock {
            if (batchBuilder.messagesCount > 0) {
                flusherFuture.cancel(false)
                batchQueue.add(batchBuilder.build())
                batchBuilder.clear()
            }
        }

        while (true) {
            val messageBuilder = messageQueue.take()
            if (messageBuilder === TERMINAL_MESSAGE) break

            messageBuilder.metadataBuilder.idBuilder.apply {
                timestamp = Instant.now().toTimestamp()
                sequence = sequenceProducer(messageBuilder)
            }

            lock.withLock {
                batchBuilder.addMessages(messageBuilder.build())
                when (batchBuilder.messagesCount) {
                    1 -> flusherFuture = batchFlusherExecutor.schedule(::enqueueBatch, maxFlushTime, maxFlushTimeUnit)
                    maxBatchSize -> enqueueBatch()
                }
            }
        }

        enqueueBatch()
        batchFlusherExecutor.shutdown()
        if (!batchFlusherExecutor.awaitTermination(TERMINATION_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            LOGGER.warn { "batchFlusherExecutor did not terminate" }
        }
        batchQueue.add(TERMINAL_BATCH)
    }

    private val batchSenderThread = thread(name = "batch-sender") {
        while (true) {
            val batch = batchQueue.take()
            if (batch === TERMINAL_BATCH) break
            onBatch(batch)
        }
    }

    fun onMessage(messageBuilder: RawMessage.Builder) {
        messageQueue.add(messageBuilder)
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
        private val TERMINAL_MESSAGE = RawMessage.newBuilder()
        private val TERMINAL_BATCH = RawMessageBatch.newBuilder().build()
        private const val TERMINATION_WAIT_TIMEOUT_MS = 5_000L
    }
}