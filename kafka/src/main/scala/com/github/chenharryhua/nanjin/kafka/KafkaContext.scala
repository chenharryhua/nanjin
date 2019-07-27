package com.github.chenharryhua.nanjin.kafka

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import scala.collection.JavaConverters._

final class KafkaContext(val settings: KafkaSettings)
    extends SerdeModule(
      new CachedSchemaRegistryClient(
        settings.schemaRegistrySettings.baseUrls.asJava,
        settings.schemaRegistrySettings.identityMapCapacity,
        settings.schemaRegistrySettings.originals.asJava,
        settings.schemaRegistrySettings.httpHeaders.asJava)) with Serializable {

  def asKey[K: SerdeOf]: KeySerde[K]     = SerdeOf[K].asKey(Map.empty)
  def asValue[V: SerdeOf]: ValueSerde[V] = SerdeOf[V].asValue(Map.empty)

  def topic[K: SerdeOf, V: SerdeOf](
    topicName: KafkaTopicName
  ): KafkaTopic[K, V] =
    new KafkaTopic[K, V](
      topicName,
      settings.fs2Settings,
      settings.akkaSettings,
      csrClient,
      asKey[K],
      asValue[V]
    )
}
