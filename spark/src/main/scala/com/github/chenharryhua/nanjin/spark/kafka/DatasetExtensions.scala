package com.github.chenharryhua.nanjin.spark.kafka

import java.time.Clock

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, NJConsumerRecord, NJProducerRecord}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession

private[kafka] trait DatasetExtensions {

  implicit final class SparKafkaTopicSyntax[F[_], K, V](topic: KafkaTopic[F, K, V])
      extends Serializable {

    def sparKafka(implicit spark: SparkSession): SparKafkaSession[K, V] =
      new SparKafkaSession(topic.description, SparKafkaParams.default)
  }

  implicit final class SparKafkaConsumerRecordSyntax[K: TypedEncoder, V: TypedEncoder](
    consumerRecords: TypedDataset[NJConsumerRecord[K, V]]
  ) {

    def nullValues: TypedDataset[NJConsumerRecord[K, V]] =
      consumerRecords.filter(consumerRecords('value).isNone)

    def nullKeys: TypedDataset[NJConsumerRecord[K, V]] =
      consumerRecords.filter(consumerRecords('key).isNone)

    def values: TypedDataset[V] =
      consumerRecords.select(consumerRecords('value)).as[Option[V]].deserialized.flatMap(x => x)

    def keys: TypedDataset[K] =
      consumerRecords.select(consumerRecords('key)).as[Option[K]].deserialized.flatMap(x => x)

    def toProducerRecords(
      conversionTactics: ConversionTactics,
      clock: Clock): TypedDataset[NJProducerRecord[K, V]] = {
      def noTS: NJProducerRecord[K, V] => NJProducerRecord[K, V] =
        NJProducerRecord.timestamp.set(Some(NJTimestamp.now(clock).milliseconds))
      val noPT: NJProducerRecord[K, V] => NJProducerRecord[K, V] =
        NJProducerRecord.partition.set(None)

      val sorted =
        consumerRecords.orderBy(consumerRecords('timestamp).asc, consumerRecords('offset).asc)

      conversionTactics match {
        case ConversionTactics(true, true) =>
          sorted.deserialized.map(_.toNJProducerRecord)
        case ConversionTactics(false, true) =>
          sorted.deserialized.map(nj => noPT(nj.toNJProducerRecord))
        case ConversionTactics(true, false) =>
          sorted.deserialized.map(nj => noTS(nj.toNJProducerRecord))
        case ConversionTactics(false, false) =>
          sorted.deserialized.map(nj => noTS.andThen(noPT)(nj.toNJProducerRecord))
      }
    }
  }
}
