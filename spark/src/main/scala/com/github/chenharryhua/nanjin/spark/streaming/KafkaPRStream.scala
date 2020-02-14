package com.github.chenharryhua.nanjin.spark.streaming

import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJProducerRecord
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset
import cats.implicits._

final class KafkaPRStream[F[_], K: TypedEncoder, V: TypedEncoder](
  ds: Dataset[NJProducerRecord[K, V]],
  params: StreamConfigF.StreamConfig)
    extends Serializable {

  @transient lazy val typedDataset: TypedDataset[NJProducerRecord[K, V]] = TypedDataset.create(ds)

  private val p: StreamParams = StreamConfigF.evalParams(params)

  def kafkaSink(kit: KafkaTopicKit[K, V]): NJKafkaSink[F] =
    new NJKafkaSink[F](
      typedDataset.deserialized
        .map(
          _.bimap(
            k => kit.codec.keySerde.serializer.serialize(kit.topicName.value, k),
            v => kit.codec.valSerde.serializer.serialize(kit.topicName.value, v)))
        .dataset
        .writeStream,
      params,
      kit.settings.producerSettings,
      kit.topicName
    )
}
