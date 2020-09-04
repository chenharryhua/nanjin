package com.github.chenharryhua.nanjin.spark.sstream

import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.kafka.NJProducerRecord
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class KafkaPrSStream[F[_], K: TypedEncoder, V: TypedEncoder](
  ds: Dataset[NJProducerRecord[K, V]],
  cfg: SStreamConfig
) extends SparkStreamUpdateParams[KafkaPrSStream[F, K, V]] {

  override def withParamUpdate(f: SStreamConfig => SStreamConfig): KafkaPrSStream[F, K, V] =
    new KafkaPrSStream[F, K, V](ds, f(cfg))

  @transient lazy val typedDataset: TypedDataset[NJProducerRecord[K, V]] = TypedDataset.create(ds)

  override val params: SStreamParams = cfg.evalConfig

  def someValues: KafkaPrSStream[F, K, V] =
    new KafkaPrSStream[F, K, V](typedDataset.filter(typedDataset('value).isNotNone).dataset, cfg)

  def kafkaSink(kit: KafkaTopic[F, K, V]): NJKafkaSink[F] =
    new NJKafkaSink[F](
      typedDataset.deserialized
        .map(_.bimap(k => kit.codec.keyCodec.encode(k), v => kit.codec.valCodec.encode(v)))
        .dataset
        .writeStream,
      cfg,
      kit.context.settings.producerSettings,
      kit.topicName)

  def sparkStream: SparkSStream[F, NJProducerRecord[K, V]] =
    new SparkSStream[F, NJProducerRecord[K, V]](ds, cfg)

}
