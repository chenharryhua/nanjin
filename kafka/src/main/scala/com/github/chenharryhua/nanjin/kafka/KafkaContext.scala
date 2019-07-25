package com.github.chenharryhua.nanjin.kafka

final case class KafkaContext(settings: KafkaSettings) {

  def topic[K: SerdeOf, V: SerdeOf](
    topicName: KafkaTopicName
  ): KafkaTopic[K, V] =
    KafkaTopic[K, V](
      topicName,
      settings.fs2Settings,
      settings.akkaSettings)
}
