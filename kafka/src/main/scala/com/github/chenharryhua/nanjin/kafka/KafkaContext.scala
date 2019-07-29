package com.github.chenharryhua.nanjin.kafka

final class KafkaContext(val settings: KafkaSettings)
    extends SerdeModule(settings.schemaRegistrySettings) with Serializable {

  def asKey[K: SerdeOf]: KeySerde[K]     = SerdeOf[K].asKey(Map.empty)
  def asValue[V: SerdeOf]: ValueSerde[V] = SerdeOf[V].asValue(Map.empty)

  def topic[K: SerdeOf, V: SerdeOf](
    topicName: KafkaTopicName
  ): KafkaTopic[K, V] =
    new KafkaTopic[K, V](
      topicName,
      settings.fs2Settings,
      settings.akkaSettings,
      settings.schemaRegistrySettings,
      asKey[K],
      asValue[V]
    )
}
