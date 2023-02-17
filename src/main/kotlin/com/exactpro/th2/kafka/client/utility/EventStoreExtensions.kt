/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

@file:JvmName("EventStoreExtensions")
package com.exactpro.th2.kafka.client.utility

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.MessageRouter
import com.fasterxml.jackson.core.JsonProcessingException
import mu.KotlinLogging

private val LOGGER = KotlinLogging.logger { }

@Throws(JsonProcessingException::class)
fun MessageRouter<EventBatch>.storeEvent(
    event: Event,
    parentEventId: EventID,
) = storeEvent(event.toProto(parentEventId))

@Throws(JsonProcessingException::class)
private fun MessageRouter<EventBatch>.storeEvent(
    protoEvent: com.exactpro.th2.common.grpc.Event,
): EventID {
    try {
        send(EventBatch.newBuilder().addEvents(protoEvent).build())
    } catch (e: Exception) {
        throw RuntimeException("Event '${protoEvent.id.id}' store failure", e)
    }
    LOGGER.debug("Event {} sent", protoEvent.id.id)
    return protoEvent.id
}