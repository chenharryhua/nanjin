package com.github.chenharryhua.nanjin.kafka

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import scala.collection.JavaConverters._

final class KafkaContext(val settings: KafkaSettings) extends Serializable {
  private val srClient: CachedSchemaRegistryClient =
    new CachedSchemaRegistryClient(
      settings.schemaRegistrySettings.baseUrls.asJava,
      settings.schemaRegistrySettings.identityMapCapacity,
      settings.schemaRegistrySettings.originals.asJava,
      settings.schemaRegistrySettings.httpHeaders.asJava)

  def topic[K: SerdeOf, V: SerdeOf](
    topicName: KafkaTopicName
  ): KafkaTopic[K, V] =
    new KafkaTopic[K, V](
      topicName,
      settings.fs2Settings,
      settings.akkaSettings,
      srClient
    )
}
