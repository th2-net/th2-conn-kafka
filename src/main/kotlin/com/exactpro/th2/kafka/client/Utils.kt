package com.exactpro.th2.kafka.client

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId

val MessageId.logId: String // TODO: move to common utils?
    get() = "${sessionAlias}:${direction.toString().lowercase()}:${timestamp}:${sequence}:${subsequence.joinToString(".")}"