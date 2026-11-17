package com.github.chenharryhua.nanjin.kafka

import scala.util.control.NoStackTrace

/** Raised when a required Schema Registry URL is not configured. */
final case class SchemaRegistryUrlAbsent(configKey: String)
    extends IllegalStateException(s"Fatal error: $configKey is absent") with NoStackTrace

/** Raised when a required key or value schema for a topic is unavailable. */
final case class TopicSchemaUnavailable(message: String)
    extends IllegalStateException(message) with NoStackTrace

/** Raised when a consumer is assigned an empty partition-offset map. */
final case class EmptyTopicPartitionMap(topicName: TopicName)
    extends RuntimeException(s"Empty partition-offset map for topic ${topicName.value}") with NoStackTrace
