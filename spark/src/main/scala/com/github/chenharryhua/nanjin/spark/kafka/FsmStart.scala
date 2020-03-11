package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.spark.streaming.{KafkaCRStream, SparkStream, StreamConfig}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.avro.Schema
import org.apache.parquet.avro.AvroSchemaConverter
import org.apache.parquet.schema.MessageType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.DataType

trait SparKafkaUpdateParams[A] extends UpdateParams[SKConfig, A] with Serializable {
  def params: SKParams
}

final class FsmStart[F[_], K, V](kit: KafkaTopicKit[F, K, V], cfg: SKConfig)(
  implicit sparkSession: SparkSession)
    extends SparKafkaUpdateParams[FsmStart[F, K, V]] {

  override def withParamUpdate(f: SKConfig => SKConfig): FsmStart[F, K, V] =
    new FsmStart[F, K, V](kit, f(cfg))

  override val params: SKParams = SKConfigF.evalConfig(cfg)

  def avroSchema: Schema         = kit.topicDef.crAvroSchema
  def sparkSchema: DataType      = SchemaConverters.toSqlType(avroSchema).dataType
  def parquetSchema: MessageType = new AvroSchemaConverter().convert(avroSchema)

  def fromKafka(implicit ev: Sync[F]): F[FsmRdd[F, K, V]] =
    sk.loadKafkaRdd(kit, params.timeRange, params.locationStrategy)
      .map(new FsmRdd[F, K, V](_, kit, cfg))

  def fromDisk(implicit ev: Sync[F]): F[FsmRdd[F, K, V]] =
    sk.loadDiskRdd[F, K, V](sk.replayPath(kit.topicName)).map(new FsmRdd[F, K, V](_, kit, cfg))

  /**
    * shorthand
    */
  def save(implicit ev: Sync[F], ev2: ContextShift[F]): F[Unit] = fromKafka.flatMap(_.save)

  def saveJackson(implicit ev: Sync[F], ev2: ContextShift[F]): F[Unit] =
    fromKafka.flatMap(_.saveJackson)

  def saveAvro(implicit ev: Sync[F], ev2: ContextShift[F]): F[Unit] =
    fromKafka.flatMap(_.saveAvro)

  def replay(implicit ev: ConcurrentEffect[F], ev1: Timer[F], ev2: ContextShift[F]): F[Unit] =
    fromDisk.flatMap(_.replay)

  def countKafka(implicit ev: Sync[F]): F[Long] = fromKafka.map(_.count)
  def countDisk(implicit ev: Sync[F]): F[Long]  = fromDisk.map(_.count)

  def pipeTo(other: KafkaTopicKit[F, K, V])(
    implicit ev: ConcurrentEffect[F],
    ev1: Timer[F],
    ev2: ContextShift[F]): F[Unit] =
    fromKafka.flatMap(_.pipeTo(other))

  /**
    * inject dataset
    */

  def crDataset(tds: TypedDataset[NJConsumerRecord[K, V]])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]) =
    new FsmConsumerRecords[F, K, V](tds.dataset, kit, cfg)

  def prDataset(tds: TypedDataset[NJProducerRecord[K, V]])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]) =
    new FsmProducerRecords[F, K, V](tds.dataset, kit, cfg)

  /**
    * streaming
    */

  def streamingPipeTo(otherTopic: KafkaTopicKit[F, K, V])(
    implicit
    concurrent: Concurrent[F],
    timer: Timer[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    streaming.flatMap(_.someValues.toProducerRecords.kafkaSink(otherTopic).showProgress)

  def streaming[A](f: NJConsumerRecord[K, V] => A)(
    implicit
    sync: Sync[F],
    encoder: TypedEncoder[A]): F[SparkStream[F, A]] =
    sk.streaming[F, K, V, A](kit, params.timeRange)(f)
      .map(s =>
        new SparkStream(
          s.dataset,
          StreamConfig(params.timeRange, params.showDs, params.fileFormat)
            .withCheckpointAppend(s"kafka/${kit.topicName.value}")))

  def streaming(
    implicit
    sync: Sync[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[KafkaCRStream[F, K, V]] =
    sk.streaming[F, K, V, NJConsumerRecord[K, V]](kit, params.timeRange)(identity)
      .map(s =>
        new KafkaCRStream[F, K, V](
          s.dataset,
          StreamConfig(params.timeRange, params.showDs, params.fileFormat)
            .withCheckpointAppend(s"kafkacr/${kit.topicName.value}")))
}
