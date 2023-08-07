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

import org.assertj.core.api.Assertions.assertThatThrownBy
import kotlin.test.Test

class ConfigTest {
    @Test
    fun `valid config`() {
        Config(
            topics = Topics(
                publish = listOf(
                    TopicConfig(
                        topic = "topic_01",
                        sessionAlias = "alias_01",
                        sessionGroup = "group_01"
                    ),
                    TopicConfig(
                        topic = "topic_02",
                        sessionAlias = "alias_02",
                        sessionGroup = "group_01"
                    ),
                    TopicConfig(
                        topic = "topic_03",
                        key = null,
                        sessionAlias = "alias_03",
                        sessionGroup = "group_02"
                    ),
                    TopicConfig(
                        topic = "topic_04",
                        key = null,
                        sessionAlias = "alias_04",
                        sessionGroup = "group_02"
                    )
                )
            )
        )
    }

    @Test
    fun `empty alias mappings`() {
        assertThatThrownBy { Config(topics = Topics()) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContainingAll("topics.publish", "topics.subscribe")
    }

    @Test
    fun `duplicated alias`() {
        assertThatThrownBy {
            Config(
                topics = Topics(
                    publish = listOf(
                        TopicConfig(topic = "topic_01", sessionAlias = "alias_01"),
                        TopicConfig(topic = "topic_02", sessionAlias = "alias_02"),
                        TopicConfig(topic = "topic_03", sessionAlias = "alias_03", key = null),
                        TopicConfig(topic = "topic_04", sessionAlias = "alias_01", key = null),
                    )
                ),
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContainingAll("Duplicated", "alias_01")
    }

    @Test
    fun `duplicated topic`() {
        assertThatThrownBy {
            Config(
                topics = Topics(
                    publish = listOf(
                        TopicConfig(topic = "topic_01", sessionAlias = "alias_01"),
                        TopicConfig(topic = "topic_02", sessionAlias = "alias_02"),
                        TopicConfig(topic = "topic_01", sessionAlias = "alias_03", key = null)
                    )
                )
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContainingAll("Duplicated", "topic_01")
    }

    @Test
    fun `duplicated stream`() {
        assertThatThrownBy {
            Config(
                topics = Topics(
                    publish = listOf(
                        TopicConfig(topic = "topic_01", sessionAlias = "alias_01", key = "key_01"),
                        TopicConfig(topic = "topic_01", sessionAlias = "alias_02", key = "key_02"),
                        TopicConfig(topic = "topic_01", sessionAlias = "alias_03", key = "key_03"),
                        TopicConfig(topic = "topic_01", sessionAlias = "alias_04", key = "key_02")
                    )
                )
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContainingAll("Duplicated", "key_02")
    }

    @Test
    fun `same alias in different session groups`() {
        assertThatThrownBy {
            Config(
                topics = Topics(
                    publish = listOf(
                        TopicConfig(topic = "topic_01", sessionAlias = "alias_01", sessionGroup = "group_01"),
                        TopicConfig(topic = "topic_02", sessionAlias = "alias_02")
                    ),
                    subscribe = listOf(
                        TopicConfig(topic = "topic_01", sessionAlias = "alias_01", sessionGroup = "group_02")
                    )
                )
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContainingAll("Different groups", "alias_01")
    }
}