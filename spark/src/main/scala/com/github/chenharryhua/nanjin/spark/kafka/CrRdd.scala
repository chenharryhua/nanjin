package com.github.chenharryhua.nanjin.spark.kafka

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.{
  CompulsoryK,
  CompulsoryKV,
  CompulsoryV,
  OptionalKV
}
import com.github.chenharryhua.nanjin.spark.{RddExt, SparkSessionExt}
import com.github.chenharryhua.nanjin.utils
import com.sksamuel.avro4s.{AvroSchema, SchemaFor, ToRecord, Encoder => AvroEncoder}
import frameless.cats.implicits.rddOps
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.{AvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

final class CrRdd[F[_], K, V](
  val rdd: RDD[OptionalKV[K, V]],
  val topicName: TopicName,
  val cfg: SKConfig)(implicit
  val sparkSession: SparkSession,
  val keyEncoder: AvroEncoder[K],
  val valEncoder: AvroEncoder[V])
    extends SparKafkaUpdateParams[CrRdd[F, K, V]] with CrRddSaveModule[F, K, V]
    with CrRddInvModule[F, K, V] {

  override def params: SKParams = cfg.evalConfig

  override def withParamUpdate(f: SKConfig => SKConfig): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd, topicName, f(cfg))

  def withTopicName(tn: String): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd, TopicName.unsafeFrom(tn), cfg)

  //transformation

  def kafkaPartition(num: Int): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.filter(_.partition === num), topicName, cfg)

  def ascending: CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.sortBy(identity, ascending = true), topicName, cfg)

  def descending: CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.sortBy(identity, ascending = false), topicName, cfg)

  def repartition(num: Int): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.repartition(num), topicName, cfg)

  def bimap[K2: AvroEncoder, V2: AvroEncoder](k: K => K2, v: V => V2): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.map(_.bimap(k, v)), topicName, cfg)

  def flatMap[K2: AvroEncoder, V2: AvroEncoder](
    f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K2, V2]]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.flatMap(f), topicName, cfg)

  def dismissNulls: CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.dismissNulls, topicName, cfg)

  def inRange(dr: NJDateTimeRange): CrRdd[F, K, V] =
    new CrRdd[F, K, V](
      rdd.filter(m => dr.isInBetween(m.timestamp)),
      topicName,
      cfg.withTimeRange(dr))

  def inRange: CrRdd[F, K, V] = inRange(params.timeRange)

  def inRange(start: String, end: String): CrRdd[F, K, V] =
    inRange(params.timeRange.withTimeRange(start, end))

  // out of FsmRdd

  def typedDataset(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): TypedDataset[OptionalKV[K, V]] =
    TypedDataset.create(rdd)

  // untyped world
  @SuppressWarnings(Array("AsInstanceOf"))
  def toDF: DataFrame = {
    implicit val ks: SchemaFor[K]        = keyEncoder.schemaFor
    implicit val vs: SchemaFor[V]        = valEncoder.schemaFor
    val toGR: ToRecord[OptionalKV[K, V]] = ToRecord[OptionalKV[K, V]]
    val avroSchema: Schema               = AvroSchema[OptionalKV[K, V]]
    val sparkDataType: DataType          = SchemaConverters.toSqlType(avroSchema).dataType
    val sparkStructType: StructType      = sparkDataType.asInstanceOf[StructType]
    val rowEnconder: ExpressionEncoder[Row] =
      RowEncoder.apply(sparkStructType).resolveAndBind()

    sparkSession.createDataFrame(
      rdd.mapPartitions { rcds =>
        val deSer: AvroDeserializer = new AvroDeserializer(avroSchema, sparkDataType)
        rcds.map { rcd =>
          rowEnconder.fromRow(deSer.deserialize(toGR.to(rcd)).asInstanceOf[InternalRow])
        }
      },
      sparkStructType
    )
  }

  def crDataset(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): CrDataset[F, K, V] =
    new CrDataset(typedDataset.dataset, cfg)

  def stream(implicit F: Sync[F]): Stream[F, OptionalKV[K, V]] =
    rdd.stream[F]

  def source(implicit F: ConcurrentEffect[F]): Source[OptionalKV[K, V], NotUsed] =
    rdd.source[F]

  // rdd
  def values: RDD[CompulsoryV[K, V]]     = rdd.flatMap(_.toCompulsoryV)
  def keys: RDD[CompulsoryK[K, V]]       = rdd.flatMap(_.toCompulsoryK)
  def keyValues: RDD[CompulsoryKV[K, V]] = rdd.flatMap(_.toCompulsoryKV)

  // pipe
  def pipeTo(otherTopic: KafkaTopic[F, K, V])(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): F[Unit] = {
    sparkSession.withGroupId(s"nj.rdd.pipe.${utils.random4d.value}")
    stream
      .map(_.toNJProducerRecord.noMeta)
      .through(sk.uploader(otherTopic, params.uploadRate))
      .map(_ => print("."))
      .compile
      .drain
  }
}
