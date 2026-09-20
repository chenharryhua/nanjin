package com.github.chenharryhua.nanjin.kafka

import scala.util.control.NoStackTrace

/** Raised when a required Schema Registry URL is not configured. */
final case class SchemaRegistryUrlAbsent(configKey: String)
    extends IllegalStateException(s"Fatal error: $configKey is absent") with NoStackTrace

/** Raised when a required schema (key or value) cannot be found for a topic. */
final case class TopicSchemaAbsent(missing: String)
    extends IllegalStateException(s"$missing") with NoStackTrace

/** Raised when local schemas are not backward compatible with broker schemas. */
final case class SchemaIncompatible(topicName: TopicName)
    extends RuntimeException(s"Schema incompatible for topic ${topicName.value}") with NoStackTrace

/** Raised when a consumer is assigned an empty partition-offset map. */
final case class EmptyTopicPartitionMap(topicName: TopicName)
    extends RuntimeException(s"Empty partition-offset map for topic ${topicName.value}") with NoStackTrace
