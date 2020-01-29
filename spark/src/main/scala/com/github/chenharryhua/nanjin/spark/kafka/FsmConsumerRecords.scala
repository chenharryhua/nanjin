package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.kafka.common.{NJConsumerRecord, NJProducerRecord}
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class FsmConsumerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  ds: Dataset[NJConsumerRecord[K, V]],
  initState: FsmInit[K, V])
    extends FsmSparKafka {

  @transient lazy val dataset: TypedDataset[NJConsumerRecord[K, V]] =
    TypedDataset.create(ds)

  def nullValuesCount(implicit ev: Sync[F]): F[Long] =
    dataset.filter(dataset('value).isNone).count[F]

  def nullKeysCount(implicit ev: Sync[F]): F[Long] =
    dataset.filter(dataset('key).isNone).count[F]

  def values: TypedDataset[V] =
    dataset.select(dataset('value)).as[Option[V]].deserialized.flatMap(x => x)

  def keys: TypedDataset[K] =
    dataset.select(dataset('key)).as[Option[K]].deserialized.flatMap(x => x)

  private def convertPRs(
    consumerRecords: TypedDataset[NJConsumerRecord[K, V]]): TypedDataset[NJProducerRecord[K, V]] = {
    def noTS: NJProducerRecord[K, V] => NJProducerRecord[K, V] =
      NJProducerRecord.timestamp.set(Some(NJTimestamp.now(initState.params.clock).milliseconds))
    val noPT: NJProducerRecord[K, V] => NJProducerRecord[K, V] =
      NJProducerRecord.partition.set(None)

    val sorted =
      consumerRecords.orderBy(consumerRecords('timestamp).asc, consumerRecords('offset).asc)

    initState.params.conversionTactics match {
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

  def show(implicit ev: Sync[F]): F[Unit] =
    dataset.show[F](initState.params.showRowNumber, initState.params.isShowTruncate)

  def save(): Unit =
    dataset.write
      .mode(initState.params.saveMode)
      .format(initState.params.fileFormat.format)
      .save(initState.params.getPath(initState.kafkaTopicDesc.topicName))

  def toProducerRecords: FsmProducerRecords[F, K, V] = initState.prDataset(convertPRs(dataset))

  def stats: FsmStatistics[F, K, V] = new FsmStatistics(ds, initState)
}
