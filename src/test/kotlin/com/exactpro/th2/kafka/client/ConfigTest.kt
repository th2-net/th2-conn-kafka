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
            aliasToTopic = mapOf(
                "alias_01" to KafkaTopic("topic_01"),
                "alias_02" to KafkaTopic("topic_02")
            ),
            aliasToTopicAndKey = mapOf(
                "alias_03" to KafkaStream("topic_03", null),
                "alias_04" to KafkaStream("topic_04", null),
            ),
            sessionGroups = mapOf(
                "group_01" to listOf("alias_01", "alias_02"),
                "group_02" to listOf("alias_03", "alias_04")
            )
        )
    }

    @Test
    fun `empty alias mappings`() {
        assertThatThrownBy { Config() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContainingAll("aliasToTopic", "aliasToTopicAndKey")
    }

    @Test
    fun `duplicated aliases`() {
        assertThatThrownBy { Config(
            aliasToTopic = mapOf("alias_01" to KafkaTopic("topic_01"), "alias_02" to KafkaTopic("topic_02")),
            aliasToTopicAndKey = mapOf("alias_03" to KafkaStream("topic_03", null), "alias_01" to KafkaStream("topic_04", null))
        ) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContainingAll("aliasToTopic", "aliasToTopicAndKey")
    }

    @Test
    fun `duplicated topic in aliasToTopic`() {
        assertThatThrownBy {
            Config(aliasToTopic = mapOf(
                "alias_01" to KafkaTopic("topic_01"),
                "alias_02" to KafkaTopic("topic_02"),
                "alias_03" to KafkaTopic("topic_01")
            ))
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContainingAll("aliasToTopic")
    }

    @Test
    fun `duplicated stream in aliasToTopicAndKey`() {
        assertThatThrownBy {
            Config(aliasToTopicAndKey = mapOf(
                "alias_01" to KafkaStream("topic_01", null),
                "alias_02" to KafkaStream("topic_02", null),
                "alias_03" to KafkaStream("topic_01", null)
            ))
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContainingAll ("aliasToTopic", "aliasToTopicAndKey")
    }

    @Test
    fun `same topics in aliasToTopicAndKey and aliasToTopic`() {
        assertThatThrownBy {
            Config(
                aliasToTopic = mapOf(
                    "alias_01" to KafkaTopic("topic_01"),
                    "alias_02" to KafkaTopic("topic_02")
                ),
                aliasToTopicAndKey = mapOf(
                    "alias_03" to KafkaStream("topic_03", null),
                    "alias_04" to KafkaStream("topic_02", null),
                    "alias_05" to KafkaStream("topic_05", null)
                )
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContainingAll ("aliasToTopic", "aliasToTopicAndKey")
    }

    @Test
    fun `duplicated alias in sessionGroups`() {
        assertThatThrownBy {
            Config(
                aliasToTopic = mapOf("alias_01" to KafkaTopic("topic_01")),
                sessionGroups = mapOf(
                    "group_01" to listOf("alias_01"),
                    "group_02" to listOf("alias_02"),
                    "group_03" to listOf("alias_03", "alias_02")
                )
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContainingAll ("sessionGroups")
    }
}