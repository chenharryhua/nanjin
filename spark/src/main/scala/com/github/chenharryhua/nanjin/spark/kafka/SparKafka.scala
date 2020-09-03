package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.messages.kafka.{NJProducerRecord, OptionalKV}
import com.github.chenharryhua.nanjin.spark.{fileSink, AvroTypedEncoder}
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.spark.sstream.{KafkaCrSStream, SStreamConfig, SparkSStream}
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.{TypedDataset, TypedEncoder}
import org.apache.avro.Schema
import org.apache.parquet.avro.AvroSchemaConverter
import org.apache.parquet.schema.MessageType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.StreamingContext
import io.circe.{Decoder => JsonDecoder}

trait SparKafkaUpdateParams[A] extends UpdateParams[SKConfig, A] with Serializable {
  def params: SKParams
}

final class SparKafka[F[_], K, V](
  val topic: KafkaTopic[F, K, V],
  val sparkSession: SparkSession,
  val cfg: SKConfig
) extends SparKafkaUpdateParams[SparKafka[F, K, V]] {

  implicit private val avroKeyEncoder: Encoder[K] = topic.topicDef.avroKeyEncoder
  implicit private val avroValEncoder: Encoder[V] = topic.topicDef.avroValEncoder
  implicit private val avroKeyDecoder: Decoder[K] = topic.topicDef.avroKeyDecoder
  implicit private val avroValDecoder: Decoder[V] = topic.topicDef.avroValDecoder
  implicit private val schemaForKey: SchemaFor[K] = topic.topicDef.keySchemaFor
  implicit private val schemaForVal: SchemaFor[V] = topic.topicDef.valSchemaFor

  implicit private val optionalKVCodec: NJAvroCodec[OptionalKV[K, V]] =
    topic.topicDef.optionalKVCodec

  implicit val ss: SparkSession = sparkSession

  override def withParamUpdate(f: SKConfig => SKConfig): SparKafka[F, K, V] =
    new SparKafka[F, K, V](topic, sparkSession, f(cfg))

  override val params: SKParams = cfg.evalConfig

  def avroSchema: Schema         = topic.topicDef.schemaForOptionalKV.schema
  def sparkSchema: DataType      = SchemaConverters.toSqlType(avroSchema).dataType
  def parquetSchema: MessageType = new AvroSchemaConverter().convert(avroSchema)

  def fromKafka(implicit sync: Sync[F]): F[CrRdd[F, K, V]] =
    sk.kafkaBatch(topic, params.timeRange, params.locationStrategy).map(crRdd)

  def fromDisk: CrRdd[F, K, V] =
    crRdd(loaders.objectFile(params.replayPath))

  /**
    * shorthand
    */
  def dump(implicit F: Sync[F], cs: ContextShift[F]): F[Long] =
    Blocker[F].use(blocker =>
      for {
        _ <- fileSink[F](blocker).delete(params.replayPath)
        cr <- fromKafka
      } yield {
        cr.rdd.saveAsObjectFile(params.replayPath)
        cr.rdd.count
      })

  def replay(implicit ce: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): F[Unit] =
    fromDisk.pipeTo(topic)

  def countKafka(implicit F: Sync[F]): F[Long] = fromKafka.flatMap(_.count)
  def countDisk(implicit F: Sync[F]): F[Long]  = fromDisk.count

  def pipeTo(other: KafkaTopic[F, K, V])(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): F[Unit] =
    fromKafka.flatMap(_.pipeTo(other))

  /**
    * rdd and dataset
    */
  def crRdd(rdd: RDD[OptionalKV[K, V]]) = new CrRdd[F, K, V](rdd, cfg)

  def crDataset(tds: TypedDataset[OptionalKV[K, V]])(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): CrDataset[F, K, V] =
    new CrDataset[F, K, V](tds.dataset, cfg)

  def crDataset(ds: Dataset[OptionalKV[K, V]])(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): CrDataset[F, K, V] =
    new CrDataset[F, K, V](ds, cfg)

  def prDataset(tds: TypedDataset[NJProducerRecord[K, V]])(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]) =
    new PrDataset[F, K, V](tds.dataset, cfg)

  /**
    * direct stream
    */

  def dstream(sc: StreamingContext): CrDStream[F, K, V] =
    new CrDStream[F, K, V](sk.kafkaDStream[F, K, V](topic, sc, params.locationStrategy), cfg)

  object load {

    object tds {

      def avro(pathStr: String)(implicit
        keyEncoder: TypedEncoder[K],
        valEncoder: TypedEncoder[V]): CrDataset[F, K, V] =
        crDataset(
          loaders.avro(pathStr)(
            AvroTypedEncoder[OptionalKV[K, V]](topic.topicDef.optionalKVCodec),
            sparkSession))

      def parquet(pathStr: String)(implicit
        keyEncoder: TypedEncoder[K],
        valEncoder: TypedEncoder[V]): CrDataset[F, K, V] =
        crDataset(
          loaders.parquet(pathStr)(
            AvroTypedEncoder[OptionalKV[K, V]](topic.topicDef.optionalKVCodec),
            sparkSession))

      def json(pathStr: String)(implicit
        keyEncoder: TypedEncoder[K],
        valEncoder: TypedEncoder[V]): CrDataset[F, K, V] =
        crDataset(
          loaders.json(pathStr)(
            AvroTypedEncoder[OptionalKV[K, V]](topic.topicDef.optionalKVCodec),
            sparkSession))
    }

    object rdd {

      def avro(pathStr: String): CrRdd[F, K, V] =
        crRdd(loaders.raw.avro[OptionalKV[K, V]](pathStr))

      def parquet(pathStr: String): CrRdd[F, K, V] =
        crRdd(loaders.raw.parquet[OptionalKV[K, V]](pathStr))

      def jackson(pathStr: String): CrRdd[F, K, V] =
        crRdd(loaders.raw.jackson[OptionalKV[K, V]](pathStr))

      def binAvro(pathStr: String): CrRdd[F, K, V] =
        crRdd(loaders.raw.binAvro[OptionalKV[K, V]](pathStr))

      def circe(pathStr: String)(implicit ev: JsonDecoder[OptionalKV[K, V]]): CrRdd[F, K, V] =
        crRdd(loaders.circe[OptionalKV[K, V]](pathStr))

    }
  }

  /**
    * structured stream
    */

  def sstream[A](f: OptionalKV[K, V] => A)(implicit
    sync: Sync[F],
    encoder: TypedEncoder[A]): SparkSStream[F, A] =
    new SparkSStream[F, A](
      sk.kafkaSStream[F, K, V, A](topic)(f).dataset,
      SStreamConfig(params.timeRange, params.showDs)
        .withCheckpointAppend(s"kafka/${topic.topicName.value}"))

  def sstream(implicit
    sync: Sync[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): KafkaCrSStream[F, K, V] =
    new KafkaCrSStream[F, K, V](
      sk.kafkaSStream[F, K, V, OptionalKV[K, V]](topic)(identity).dataset,
      SStreamConfig(params.timeRange, params.showDs)
        .withCheckpointAppend(s"kafkacr/${topic.topicName.value}"))
}
