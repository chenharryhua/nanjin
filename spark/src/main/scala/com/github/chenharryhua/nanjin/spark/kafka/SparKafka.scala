package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.{NJFileFormat, UpdateParams}
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.{NJProducerRecord, OptionalKV}
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.spark.streaming.{KafkaCRStream, SparkStream, StreamConfig}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.{SparkDelay, TypedDataset, TypedEncoder}
import io.circe.generic.auto._
import io.circe.{Decoder => JsonDecoder}
import org.apache.avro.Schema
import org.apache.parquet.avro.AvroSchemaConverter
import org.apache.parquet.schema.MessageType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.DataType

trait SparKafkaUpdateParams[A] extends UpdateParams[SKConfig, A] with Serializable {
  def params: SKParams
}

final class SparKafka[F[_], K, V](topic: KafkaTopic[F, K, V], cfg: SKConfig)(implicit
  sparkSession: SparkSession)
    extends SparKafkaUpdateParams[SparKafka[F, K, V]] {

  override def withParamUpdate(f: SKConfig => SKConfig): SparKafka[F, K, V] =
    new SparKafka[F, K, V](topic, f(cfg))

  override val params: SKParams = cfg.evalConfig

  def withTopicName(tn: String): SparKafka[F, K, V] =
    new SparKafka[F, K, V](topic.withTopicName(tn), cfg)

  def avroSchema: Schema         = topic.topicDef.schemaFor.schema
  def sparkSchema: DataType      = SchemaConverters.toSqlType(avroSchema).dataType
  def parquetSchema: MessageType = new AvroSchemaConverter().convert(avroSchema)

  def fromKafka(implicit sync: Sync[F]): F[CrRdd[F, K, V]] =
    sk.unloadKafka(topic, params.timeRange, params.locationStrategy)
      .map(
        new CrRdd[F, K, V](_, topic.topicName, cfg)(
          topic.topicDef.avroKeyEncoder,
          topic.topicDef.avroValEncoder,
          sparkSession))

  def fromDisk: CrRdd[F, K, V] =
    new CrRdd[F, K, V](
      sk.loadDiskRdd[K, V](params.replayPath(topic.topicName)),
      topic.topicName,
      cfg)(topic.topicDef.avroKeyEncoder, topic.topicDef.avroValEncoder, sparkSession)

  /**
    * shorthand
    */
  def dump(implicit F: Sync[F], cs: ContextShift[F]): F[Long] =
    fromKafka.flatMap(_.dump)

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
    new CrDataset[F, K, V](tds.dataset, cfg)(keyEncoder, valEncoder)

  def readAvro(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): CrDataset[F, K, V] =
    crDataset(sparkSession.avro[OptionalKV[K, V]](pathStr))

  def readAvro(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): CrDataset[F, K, V] =
    readAvro(params.pathBuilder(topic.topicName, NJFileFormat.Avro))

  def readParquet(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): CrDataset[F, K, V] =
    crDataset(sparkSession.parquet[OptionalKV[K, V]](pathStr))

  def readParquet(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): CrDataset[F, K, V] =
    readParquet(params.pathBuilder(topic.topicName, NJFileFormat.Parquet))

  def readJson(pathStr: String)(implicit
    jsonKeyDecoder: JsonDecoder[K],
    jsonValDecoder: JsonDecoder[V]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](sparkSession.json[OptionalKV[K, V]](pathStr), topic.topicName, cfg)(
      topic.topicDef.avroKeyEncoder,
      topic.topicDef.avroValEncoder,
      sparkSession)

  def readJson(implicit
    jsonKeyDecoder: JsonDecoder[K],
    jsonValDecoder: JsonDecoder[V]): CrRdd[F, K, V] =
    readJson(params.pathBuilder(topic.topicName, NJFileFormat.Json))

  def readJackson(pathStr: String): CrRdd[F, K, V] = {
    import topic.topicDef.{avroKeyDecoder, avroValDecoder}

    new CrRdd[F, K, V](sparkSession.jackson[OptionalKV[K, V]](pathStr), topic.topicName, cfg)(
      topic.topicDef.avroKeyEncoder,
      topic.topicDef.avroValEncoder,
      sparkSession)
  }

  def readJackson: CrRdd[F, K, V] =
    readJackson(params.pathBuilder(topic.topicName, NJFileFormat.Jackson))

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
          StreamConfig(params.timeRange, params.showDs)
            .withCheckpointAppend(s"kafka/${topic.topicName.value}")))

  def streaming(implicit
    sync: Sync[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[KafkaCRStream[F, K, V]] =
    sk.streaming[F, K, V, OptionalKV[K, V]](topic, params.timeRange)(identity)
      .map(s =>
        new KafkaCRStream[F, K, V](
          s.dataset,
          StreamConfig(params.timeRange, params.showDs)
            .withCheckpointAppend(s"kafkacr/${topic.topicName.value}")))
}
