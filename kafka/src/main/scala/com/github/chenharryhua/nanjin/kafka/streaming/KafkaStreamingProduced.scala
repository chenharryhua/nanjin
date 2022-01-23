package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.processor.{RecordContext, StreamPartitioner, TopicNameExtractor}
import org.apache.kafka.streams.scala.kstream.Produced

final class KafkaStreamingProduced[F[_], K, V] private[kafka] (
  topic: KafkaTopic[F, K, V],
  produced: Produced[K, V],
  topicNameExtractor: TopicNameExtractor[K, V])
    extends Produced[K, V](produced) with TopicNameExtractor[K, V] {

  val serdeVal: Serde[V]   = valueSerde
  val serdeKey: Serde[K]   = keySerde
  val topicName: TopicName = topic.topicName

  def withTopicName(topicName: TopicName): KafkaStreamingProduced[F, K, V] =
    new KafkaStreamingProduced[F, K, V](topic.withTopicName(topicName), produced, topicNameExtractor)

  def withTopicNameExtractor(topicNameExtractor: TopicNameExtractor[K, V]): KafkaStreamingProduced[F, K, V] =
    new KafkaStreamingProduced[F, K, V](topic, produced, topicNameExtractor)

  override def extract(key: K, value: V, rc: RecordContext): String = topicNameExtractor.extract(key, value, rc)

  private def update(produced: Produced[K, V]): KafkaStreamingProduced[F, K, V] =
    new KafkaStreamingProduced[F, K, V](topic, produced, topicNameExtractor)

  override def withStreamPartitioner(partitioner: StreamPartitioner[? >: K, ? >: V]): KafkaStreamingProduced[F, K, V] =
    update(produced.withStreamPartitioner(partitioner))

  override def withName(name: String): KafkaStreamingProduced[F, K, V] =
    update(produced.withName(name))

  override def withValueSerde(valueSerde: Serde[V]): KafkaStreamingProduced[F, K, V] =
    update(produced.withValueSerde(valueSerde))

  override def withKeySerde(keySerde: Serde[K]): KafkaStreamingProduced[F, K, V] =
    update(produced.withKeySerde(keySerde))
}
