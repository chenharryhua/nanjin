package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import com.github.chenharryhua.nanjin.kafka.codec.iso
import com.github.chenharryhua.nanjin.kafka.common.NJProducerRecord
import com.github.chenharryhua.nanjin.spark._
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.kafka.{produce, ProducerRecords, ProducerResult}
import fs2.{Chunk, Stream}
import org.apache.spark.sql.Dataset

final class FsmProducerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  prs: Dataset[NJProducerRecord[K, V]],
  initState: FsmInit[K, V]
) extends FsmSparKafka {

  @transient lazy val dataset: TypedDataset[NJProducerRecord[K, V]] =
    TypedDataset.create(prs)

  def upload(
    implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    dataset
      .repartition(initState.params.repartition)
      .stream[F]
      .chunkN(initState.params.uploadRate.batchSize)
      .metered(initState.params.uploadRate.duration)
      .map(chk =>
        ProducerRecords[Chunk, K, V](
          chk.map(d => iso.isoFs2ProducerRecord[K, V].reverseGet(d.toProducerRecord))))
      .through(produce(initState.kafkaTopicDesc.fs2ProducerSettings[F]))

  def show(implicit ev: Sync[F]): F[Unit] =
    dataset.show[F](initState.params.showRowNumber, initState.params.isShowTruncate)

}
