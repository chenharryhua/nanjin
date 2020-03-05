package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.spark.streaming.{KafkaCRStream, SparkStream, StreamConfig}
import frameless.{TypedDataset, TypedEncoder}
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

  def avroSchema: Schema    = kit.topicDef.njAvroSchema
  def sparkSchema: DataType = SchemaConverters.toSqlType(avroSchema).dataType

  def fromKafka[F[_]: Sync]: F[FsmRdd[F, K, V]] =
    sk.loadKafkaRdd(kit, params.timeRange, params.locationStrategy)
      .map(new FsmRdd[F, K, V](_, kit, cfg))

  def fromDisk[F[_]: Sync]: F[FsmRdd[F, K, V]] =
    sk.loadDiskRdd[F, K, V](sk.replayPath(kit.topicName)).map(new FsmRdd[F, K, V](_, kit, cfg))

  /**
    * shorthand
    */
  def save[F[_]: Sync: ContextShift]: F[Unit]        = fromKafka[F].flatMap(_.save)
  def saveJackson[F[_]: Sync: ContextShift]: F[Unit] = fromKafka[F].flatMap(_.saveJackson)

  def replay[F[_]: ConcurrentEffect: Timer: ContextShift]: F[Unit] = fromDisk[F].flatMap(_.replay)

  def countKafka[F[_]: Sync]: F[Long] = fromKafka[F].map(_.count)
  def countDisk[F[_]: Sync]: F[Long]  = fromDisk[F].map(_.count)

  def pipeTo[F[_]: ConcurrentEffect: Timer: ContextShift](other: KafkaTopicKit[K, V]): F[Unit] =
    fromKafka[F].flatMap(_.pipeTo(other))

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
    * streaming
    */

  def streamingPipeTo[F[_]: Concurrent: Timer](otherTopic: KafkaTopicKit[K, V])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    streaming[F].flatMap(_.someValues.toProducerRecords.kafkaSink(otherTopic).showProgress)

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
