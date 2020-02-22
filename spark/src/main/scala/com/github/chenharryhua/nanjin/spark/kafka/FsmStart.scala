package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.spark.streaming.{KafkaCRStream, SparkStream, StreamConfig}
import com.github.chenharryhua.nanjin.utils.Keyboard
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import fs2.kafka.{produce, ProducerRecords}
import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.DataType

trait SparKafkaUpdateParams[A] extends UpdateParams[SKConfig, A] with Serializable {
  def params: SKParams
}

final class FsmStart[K, V](kit: KafkaTopicKit[K, V], cfg: SKConfig)(
  implicit sparkSession: SparkSession)
    extends SparKafkaUpdateParams[FsmStart[K, V]] {

  override def withParamUpdate(f: SKConfig => SKConfig): FsmStart[K, V] =
    new FsmStart[K, V](kit, f(cfg))

  override val params: SKParams = SKConfigF.evalConfig(cfg)

  def avroSchema: Schema    = kit.topicDef.njConsumerRecordSchema
  def sparkSchema: DataType = SchemaConverters.toSqlType(avroSchema).dataType

  def fromKafka[F[_]: Sync]: FsmKafkaUnload[F, K, V] =
    new FsmKafkaUnload[F, K, V](kit, cfg)

  def fromDisk[F[_]]: FsmDiskLoad[F, K, V] =
    new FsmDiskLoad[F, K, V](kit, cfg)

  /**
    * inject dataset
    */

  def crDataset[F[_]](tds: TypedDataset[NJConsumerRecord[K, V]])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]) =
    new FsmConsumerRecords[F, K, V](tds.dataset, kit, cfg)

  def prDataset[F[_]](tds: TypedDataset[NJProducerRecord[K, V]])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]) =
    new FsmProducerRecords[F, K, V](tds.dataset, kit, cfg)

  /**
    * replay
    */
  private val replayPath: String = s"./data/replay/${kit.topicName}"

  def saveRDD[F[_]: Sync]: F[Unit] =
    sk.saveKafkaRDD(replayPath, kit, params.timeRange, params.locationStrategy)

  def crDiskStream[F[_]: Sync]: Stream[F, NJConsumerRecord[K, V]] =
    sk.loadDiskRDD[F, K, V](replayPath, params.repartition)

  def replay[F[_]: ConcurrentEffect: Timer: ContextShift]: F[Unit] = {
    val run: Stream[F, Unit] = for {
      kb <- Keyboard.signal[F]
      _ <- crDiskStream
        .chunkN(params.uploadRate.batchSize)
        .metered(params.uploadRate.duration)
        .pauseWhen(kb.map(_.contains(Keyboard.pauSe)))
        .interruptWhen(kb.map(_.contains(Keyboard.Quit)))
        .map(chk =>
          ProducerRecords(chk.map(_.toNJProducerRecord.noMeta.toFs2ProducerRecord(kit.topicName))))
        .through(produce(kit.fs2ProducerSettings[F]))
        .map(_ => print("."))
    } yield ()
    run.compile.drain
  }

  /**
    * pipeTo
    */

  def batchPipeTo[F[_]: ConcurrentEffect: Timer: ContextShift](
    otherTopic: KafkaTopicKit[K, V]): F[Unit] =
    fromKafka[F].crStream.chunks
      .map(chk =>
        ProducerRecords(
          chk.map(_.toNJProducerRecord.noMeta.toFs2ProducerRecord(otherTopic.topicName))))
      .through(produce(otherTopic.fs2ProducerSettings[F]))
      .map(_ => print("."))
      .compile
      .drain

  def streamingPipeTo[F[_]: Concurrent: Timer](otherTopic: KafkaTopicKit[K, V])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    streaming[F].flatMap(_.someValues.toProducerRecords.kafkaSink(otherTopic).showProgress)

  /**
    * streaming
    */
  def streaming[F[_]: Sync, A](f: NJConsumerRecord[K, V] => A)(
    implicit
    encoder: TypedEncoder[A]): F[SparkStream[F, A]] =
    sk.streaming[F, K, V, A](kit, params.timeRange)(f)
      .map(s =>
        new SparkStream(
          s.dataset,
          StreamConfig(params.timeRange, params.showDs, params.fileFormat)
            .withCheckpointAppend(s"kafka/${kit.topicName.value}")))

  def streaming[F[_]: Sync](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[KafkaCRStream[F, K, V]] =
    sk.streaming[F, K, V, NJConsumerRecord[K, V]](kit, params.timeRange)(identity)
      .map(s =>
        new KafkaCRStream[F, K, V](
          s.dataset,
          StreamConfig(params.timeRange, params.showDs, params.fileFormat)
            .withCheckpointAppend(s"kafkacr/${kit.topicName.value}")))
}
