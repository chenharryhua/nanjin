package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.{NJProducerRecord, OptionalKV}
import com.github.chenharryhua.nanjin.spark.streaming.{KafkaCRStream, NJStreamConfig, SparkStream}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.{SparkDelay, TypedDataset, TypedEncoder}
import org.apache.avro.Schema
import org.apache.parquet.avro.AvroSchemaConverter
import org.apache.parquet.schema.MessageType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.DataType

trait SparKafkaUpdateParams[A] extends UpdateParams[SKConfig, A] with Serializable {
  def params: SKParams
}

final class SparKafka[F[_], K, V](val topic: KafkaTopic[F, K, V], val cfg: SKConfig)(implicit
  val sparkSession: SparkSession)
    extends SparKafkaUpdateParams[SparKafka[F, K, V]] with SparKafkaReadModule[F, K, V] {

  override def withParamUpdate(f: SKConfig => SKConfig): SparKafka[F, K, V] =
    new SparKafka[F, K, V](topic, f(cfg))

  override val params: SKParams = cfg.evalConfig

  def withTopicName(tn: String): SparKafka[F, K, V] =
    new SparKafka[F, K, V](topic.withTopicName(tn), cfg)

  def avroSchema: Schema         = topic.topicDef.schemaFor.schema
  def sparkSchema: DataType      = SchemaConverters.toSqlType(avroSchema).dataType
  def parquetSchema: MessageType = new AvroSchemaConverter().convert(avroSchema)

  /**
    * shorthand
    */
  def dump(implicit F: Sync[F], cs: ContextShift[F]): F[Long] =
    Blocker[F].use(blocker =>
      fromKafka.flatMap(cr =>
        cr.save.dump(params.replayPath(topic.topicName)).run(blocker).flatMap(_ => cr.count)))

  def replay(implicit ce: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): F[Unit] =
    fromDisk.pipeTo(topic)

  def countKafka(implicit F: Sync[F]): F[Long]      = fromKafka.flatMap(_.count)
  def countDisk(implicit F: SparkDelay[F]): F[Long] = fromDisk.count

  def pipeTo(other: KafkaTopic[F, K, V])(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): F[Unit] =
    fromKafka.flatMap(_.pipeTo(other))

  /**
    * inject dataset
    */

  def crDataset(tds: TypedDataset[OptionalKV[K, V]])(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): CrDataset[F, K, V] =
    new CrDataset[F, K, V](tds.dataset, cfg)(
      keyEncoder,
      topic.topicDef.avroKeyEncoder,
      valEncoder,
      topic.topicDef.avroValEncoder)

  def prDataset(tds: TypedDataset[NJProducerRecord[K, V]])(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]) =
    new PrDataset[F, K, V](tds.dataset, cfg)

  /**
    * streaming
    */

  def streamingPipeTo(otherTopic: KafkaTopic[F, K, V])(implicit
    concurrent: Concurrent[F],
    timer: Timer[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    streaming.flatMap(_.someValues.toProducerRecords.kafkaSink(otherTopic).showProgress)

  def streaming[A](f: OptionalKV[K, V] => A)(implicit
    sync: Sync[F],
    encoder: TypedEncoder[A]): F[SparkStream[F, A]] =
    sk.streaming[F, K, V, A](topic, params.timeRange)(f)
      .map(s =>
        new SparkStream(
          s.dataset,
          NJStreamConfig(params.timeRange, params.showDs)
            .withCheckpointAppend(s"kafka/${topic.topicName.value}")))

  def streaming(implicit
    sync: Sync[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[KafkaCRStream[F, K, V]] =
    sk.streaming[F, K, V, OptionalKV[K, V]](topic, params.timeRange)(identity)
      .map(s =>
        new KafkaCRStream[F, K, V](
          s.dataset,
          NJStreamConfig(params.timeRange, params.showDs)
            .withCheckpointAppend(s"kafkacr/${topic.topicName.value}")))
}
