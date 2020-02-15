package com.github.chenharryhua.nanjin.spark.streaming

import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJProducerRecord
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class KafkaPRStream[F[_], K: TypedEncoder, V: TypedEncoder](
  ds: Dataset[NJProducerRecord[K, V]],
  cfg: StreamConfig
) extends SparkStreamUpdateParams[KafkaPRStream[F, K, V]] {

  override def withParamUpdate(f: StreamConfig => StreamConfig): KafkaPRStream[F, K, V] =
    new KafkaPRStream[F, K, V](ds, f(cfg))

  @transient lazy val typedDataset: TypedDataset[NJProducerRecord[K, V]] = TypedDataset.create(ds)

  override val params: StreamParams = StreamConfigF.evalConfig(cfg)

  def kafkaSink(kit: KafkaTopicKit[K, V]): NJKafkaSink[F] =
    new NJKafkaSink[F](
      typedDataset.deserialized
        .map(
          _.bimap(
            k => kit.codec.keySerde.serializer.serialize(kit.topicName.value, k),
            v => kit.codec.valSerde.serializer.serialize(kit.topicName.value, v)))
        .dataset
        .writeStream,
      cfg,
      kit.settings.producerSettings,
      kit.topicName)

  def sparkStream: SparkStream[F, NJProducerRecord[K, V]] =
    new SparkStream[F, NJProducerRecord[K, V]](ds, cfg)

}
