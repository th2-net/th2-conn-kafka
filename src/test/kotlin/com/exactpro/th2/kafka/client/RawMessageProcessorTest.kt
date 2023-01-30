package com.exactpro.th2.kafka.client

import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.util.toInstant
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.data.Percentage
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.EnumMap
import java.util.Random
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class RawMessageProcessorTest {

    @Test
    fun batchingTest() {
        val generatorThreads = 5
        val messagesPerThread = 5000
        val maxBatchSize = 20

        val outputBatches = sendMessages(
            generatorThreads,
            messagesPerThread,
            maxBatchSize,
            maxFlushTime = Long.MAX_VALUE
        )

        assertThat(outputBatches.sumOf { (batch, _) -> batch.messagesCount })
            .describedAs("Wrong total number of messages returned")
            .isEqualTo(generatorThreads * messagesPerThread)

        outputBatches.asSequence()
            .map { (batch, _) -> batch }
            .groupBy { it.getMessages(0).metadata.id.connectionId.sessionGroup }
            .forEach { (sessionGroup, batches) -> validateBatchList(sessionGroup, batches, maxBatchSize) }
    }

    private fun sendMessages(
        generatorThreads: Int,
        messagesPerThread: Int,
        maxBatchSize: Int,
        maxFlushTime: Long,
        sendInterval: Long = 0L
    ): List<Pair<RawMessageBatch, Long>> {
        val firstSequence: () -> Long = createSequence()
        val secondSequence: () -> Long = createSequence()
        val sequenceProducer: (RawMessage.Builder) -> Long = {
            when (it.direction) {
                Direction.FIRST -> firstSequence()
                Direction.SECOND -> secondSequence()
                else -> error("Unrecognized direction")
            }
        }

        val rnd = Random(0)
        val outputBatches: MutableList<Pair<RawMessageBatch, Long>> = ArrayList()

        RawMessageProcessor(maxBatchSize, maxFlushTime, TimeUnit.MILLISECONDS, sequenceProducer) {
            outputBatches += it to Instant.now().toEpochMilli()
        }.use { batcher ->

            val senderExecutor = Executors.newFixedThreadPool(generatorThreads)

            fun sendMessages() = repeat(messagesPerThread) {
                val msgBuilder = RawMessage.newBuilder()
                    .setMetadata(
                        RawMessageMetadata.newBuilder()
                            .setId(
                                MessageID.newBuilder()
                                    .setDirection(Direction.forNumber(rnd.nextInt(2)))
                                    .setConnectionId(
                                        ConnectionID.newBuilder()
                                            .setSessionGroup(SESSION_GROUPS[rnd.nextInt(SESSION_GROUPS.size)])
                                    )
                            )
                    )
                batcher.onMessage(msgBuilder)
                Thread.sleep(sendInterval)
            }

            repeat(generatorThreads) {
                senderExecutor.submit(::sendMessages)
            }
            senderExecutor.shutdown()
            senderExecutor.awaitTermination(5, TimeUnit.SECONDS)
        }

        return outputBatches
    }

    private fun validateBatchList(sessionGroup: String, batches: List<RawMessageBatch>, maxBatchSize: Int) {
        var prevTimestamp = 0L
        val currentSequences: MutableMap<Direction, Long> = EnumMap(Direction::class.java)
        currentSequences[Direction.FIRST] = 0
        currentSequences[Direction.SECOND] = 0

        batches.forEachIndexed { index, batch ->
            if (index < batches.lastIndex) {
                assertThat(batch.messagesCount)
                    .describedAs("Wrong batch size")
                    .isEqualTo(maxBatchSize)
            } else {
                assertThat(batch.messagesCount)
                    .describedAs("Wrong last batch size")
                    .isLessThanOrEqualTo(maxBatchSize)
                    .isGreaterThan(0)
            }

            batch.messagesList.forEach { message ->

                // verify proper grouping
                assertThat(message.metadata.id.connectionId.sessionGroup)
                    .describedAs("Wrong session alias group")
                    .isEqualTo(sessionGroup)

                // verify sequence ordering
                val prevMessageSequence = currentSequences[message.direction]
                assertThat(message.sequence)
                    .describedAs("Sequence number should be greater than previous")
                    .isGreaterThan(prevMessageSequence)
                currentSequences[message.direction] = message.sequence

                // verify timestamp ordering
                val timestamp = with(message.metadata.id.timestamp) {
                    TimeUnit.SECONDS.toNanos(seconds) + nanos
                }
                assertThat(timestamp)
                    .describedAs("Timestamp is lower than previous")
                    .isGreaterThanOrEqualTo(prevTimestamp)

                prevTimestamp = timestamp
            }
        }
    }

    @Test
    fun flushingTest() {
        val maxFlushTime = 200L

        val outputBatches = sendMessages(
            generatorThreads = 1,
            messagesPerThread = 5000,
            maxBatchSize = 5000,
            maxFlushTime,
            sendInterval = 3
        )

        outputBatches
            .groupBy { (batch, _) -> batch.getMessages(0).metadata.id.connectionId.sessionGroup }
            .forEach { (_, batchesWithTimestamp) ->
                batchesWithTimestamp.asSequence()
                    .forEachIndexed { index, (batch, endTimestamp) ->
                        val startTimestamp = batch.getMessages(0).metadata.id.timestamp.toInstant().toEpochMilli()
                        val flushTime = endTimestamp - startTimestamp
                        if (index < batchesWithTimestamp.lastIndex) {
                            assertThat(flushTime)
                                .describedAs("Batch flush time")
                                .isGreaterThanOrEqualTo(maxFlushTime)
                                .isCloseTo(maxFlushTime, Percentage.withPercentage(5.0))
                        } else {
                            assertThat(endTimestamp - startTimestamp)
                                .describedAs("Last batch flush time")
                                .isLessThan((maxFlushTime * 1.05).toLong())
                        }
                    }
            }
    }

    companion object {
        private val SESSION_GROUPS = arrayOf("group_01", "group_02", "group_03")
    }
}