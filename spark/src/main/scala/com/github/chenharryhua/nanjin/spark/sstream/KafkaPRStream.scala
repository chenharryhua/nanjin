package com.github.chenharryhua.nanjin.spark.sstream

import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class KafkaPRStream[F[_], K: TypedEncoder, V: TypedEncoder](
  ds: Dataset[NJProducerRecord[K, V]],
  cfg: NJStreamConfig
) extends SparkStreamUpdateParams[KafkaPRStream[F, K, V]] {

  override def withParamUpdate(f: NJStreamConfig => NJStreamConfig): KafkaPRStream[F, K, V] =
    new KafkaPRStream[F, K, V](ds, f(cfg))

  @transient lazy val typedDataset: TypedDataset[NJProducerRecord[K, V]] = TypedDataset.create(ds)

  override val params: NJStreamParams = cfg.evalConfig

  def someValues: KafkaPRStream[F, K, V] =
    new KafkaPRStream[F, K, V](typedDataset.filter(typedDataset('value).isNotNone).dataset, cfg)

  def kafkaSink(kit: KafkaTopic[F, K, V]): NJKafkaSink[F] =
    new NJKafkaSink[F](
      typedDataset.deserialized
        .map(_.bimap(k => kit.codec.keyCodec.encode(k), v => kit.codec.valCodec.encode(v)))
        .dataset
        .writeStream,
      cfg,
      kit.settings.producerSettings,
      kit.topicName)

  def sparkStream: NJSparkStream[F, NJProducerRecord[K, V]] =
    new NJSparkStream[F, NJProducerRecord[K, V]](ds, cfg)

}
