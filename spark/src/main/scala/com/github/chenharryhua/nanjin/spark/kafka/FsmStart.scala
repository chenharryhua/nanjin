package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
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

final class FsmStart[F[_], K, V](topic: KafkaTopic[F, K, V], cfg: SKConfig)(implicit
  sparkSession: SparkSession)
    extends SparKafkaUpdateParams[FsmStart[F, K, V]] {

  override def withParamUpdate(f: SKConfig => SKConfig): FsmStart[F, K, V] =
    new FsmStart[F, K, V](topic, f(cfg))

  override val params: SKParams = cfg.evalConfig

  def avroSchema: Schema         = topic.topicDef.schemaFor.schema
  def sparkSchema: DataType      = SchemaConverters.toSqlType(avroSchema).dataType
  def parquetSchema: MessageType = new AvroSchemaConverter().convert(avroSchema)

  def fromKafka(implicit sync: Sync[F]): F[FsmRdd[F, K, V]] =
    sk.loadKafkaRdd(topic, params.timeRange, params.locationStrategy)
      .map(new FsmRdd[F, K, V](_, topic, cfg))

  def fromDisk(implicit sync: Sync[F]): F[FsmRdd[F, K, V]] =
    sk.loadDiskRdd[F, K, V](params.replayPath(topic.topicName))
      .map(rdd =>
        new FsmRdd[F, K, V](rdd.filter(m => params.timeRange.isInBetween(m.timestamp)), topic, cfg))

  /**
    * shorthand
    */
  def dump(implicit sync: Sync[F], cs: ContextShift[F]): F[Unit] =
    fromKafka.flatMap(_.dump)

  def replay(implicit ce: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): F[Unit] =
    fromDisk.flatMap(_.replay)

  def countKafka(implicit F: Sync[F]): F[Long] = fromKafka.map(_.count)
  def countDisk(implicit F: Sync[F]): F[Long]  = fromDisk.map(_.count)

  def pipeTo(other: KafkaTopic[F, K, V])(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): F[Unit] =
    fromKafka.flatMap(_.pipeTo(other))

  /**
    * inject dataset
    */

  def crDataset(tds: TypedDataset[NJConsumerRecord[K, V]])(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]) =
    new FsmConsumerRecords[F, K, V](tds.dataset, topic, cfg)

  def prDataset(tds: TypedDataset[NJProducerRecord[K, V]])(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]) =
    new FsmProducerRecords[F, K, V](tds.dataset, topic, cfg)

  /**
    * streaming
    */

  def streamingPipeTo(otherTopic: KafkaTopic[F, K, V])(implicit
    concurrent: Concurrent[F],
    timer: Timer[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    streaming.flatMap(_.someValues.toProducerRecords.kafkaSink(otherTopic).showProgress)

  def streaming[A](f: NJConsumerRecord[K, V] => A)(implicit
    sync: Sync[F],
    encoder: TypedEncoder[A]): F[SparkStream[F, A]] =
    sk.streaming[F, K, V, A](topic, params.timeRange)(f)
      .map(s =>
        new SparkStream(
          s.dataset,
          StreamConfig(params.timeRange, params.showDs)
            .withCheckpointAppend(s"kafka/${topic.topicName.value}")))

  def streaming(implicit
    sync: Sync[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[KafkaCRStream[F, K, V]] =
    sk.streaming[F, K, V, NJConsumerRecord[K, V]](topic, params.timeRange)(identity)
      .map(s =>
        new KafkaCRStream[F, K, V](
          s.dataset,
          StreamConfig(params.timeRange, params.showDs)
            .withCheckpointAppend(s"kafkacr/${topic.topicName.value}")))
}
