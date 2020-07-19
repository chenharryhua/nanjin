package com.github.chenharryhua.nanjin.spark.kafka

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.implicits._
import cats.{Eq, Show}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.{
  CompulsoryK,
  CompulsoryKV,
  CompulsoryV,
  OptionalKV
}
import com.github.chenharryhua.nanjin.pipes.{GenericRecordEncoder, JacksonSerialization}
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt, SparkSessionExt}
import com.github.chenharryhua.nanjin.utils
import com.sksamuel.avro4s.{AvroSchema, SchemaFor, Encoder => AvroEncoder}
import frameless.cats.implicits.rddOps
import frameless.{SparkDelay, TypedDataset, TypedEncoder}
import fs2.Stream
import io.circe.generic.auto._
import io.circe.{Encoder => JsonEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Queue
import scala.reflect.runtime.universe.TypeTag

final class CrRdd[F[_], K, V](val rdd: RDD[OptionalKV[K, V]], topicName: TopicName, cfg: SKConfig)(
  implicit
  sparkSession: SparkSession,
  keyEncoder: AvroEncoder[K],
  valEncoder: AvroEncoder[V])
    extends SparKafkaUpdateParams[CrRdd[F, K, V]] {

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

  def toDF(implicit ev: TypeTag[K], ev2: TypeTag[V]): DataFrame =
    sparkSession.createDataFrame(rdd)

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

  // investigation
  def count(implicit F: SparkDelay[F]): F[Long] = {
    sparkSession.withGroupId(s"nj.rdd.count.${utils.random4d.value}")
    F.delay(rdd.count())
  }

  def stats: Statistics[F] = {
    sparkSession.withGroupId(s"nj.rdd.stats.${utils.random4d.value}")
    new Statistics[F](TypedDataset.create(rdd.map(CRMetaInfo(_))).dataset, cfg)
  }

  def malOrder(implicit F: Sync[F]): Stream[F, OptionalKV[K, V]] =
    stream.sliding(2).mapFilter {
      case Queue(a, b) => if (a.timestamp <= b.timestamp) None else Some(a)
    }

  def missingData: TypedDataset[CRMetaInfo] = {
    sparkSession.withGroupId(s"nj.rdd.miss.${utils.random4d.value}")
    inv.missingData(TypedDataset.create(values.map(CRMetaInfo(_))))
  }

  def dupRecords: TypedDataset[DupResult] = {
    sparkSession.withGroupId(s"nj.rdd.dup.${utils.random4d.value}")
    inv.dupRecords(TypedDataset.create(values.map(CRMetaInfo(_))))
  }

  def showJackson(rs: Array[OptionalKV[K, V]])(implicit F: Sync[F]): F[Unit] = {
    implicit val ks: SchemaFor[K] = keyEncoder.schemaFor
    implicit val vs: SchemaFor[V] = valEncoder.schemaFor

    val pipe: JacksonSerialization[F] = new JacksonSerialization[F](AvroSchema[OptionalKV[K, V]])
    val gre: GenericRecordEncoder[F, OptionalKV[K, V]] =
      new GenericRecordEncoder[F, OptionalKV[K, V]]()

    Stream
      .emits(rs)
      .covary[F]
      .through(gre.encode)
      .through(pipe.compactJson)
      .showLinesStdOut
      .compile
      .drain
  }

  def find(f: OptionalKV[K, V] => Boolean)(implicit F: Sync[F]): F[Unit] = {
    sparkSession.withGroupId(s"nj.rdd.find.${utils.random4d.value}")
    showJackson(rdd.filter(f).take(params.showDs.rowNum))
  }

  def show(implicit F: Sync[F]): F[Unit] =
    showJackson(rdd.take(params.showDs.rowNum))

  def diff(other: RDD[OptionalKV[K, V]])(implicit ek: Eq[K], ev: Eq[V]): RDD[DiffResult[K, V]] = {
    sparkSession.withGroupId(s"nj.rdd.diff.pos.${utils.random4d.value}")
    inv.diffRdd(rdd, other)
  }

  def diff(other: CrRdd[F, K, V])(implicit ek: Eq[K], ev: Eq[V]): RDD[DiffResult[K, V]] =
    diff(other.rdd)

  def kvDiff(other: RDD[OptionalKV[K, V]]): RDD[KvDiffResult[K, V]] = {
    sparkSession.withGroupId(s"nj.rdd.diff.kv.${utils.random4d.value}")
    inv.kvDiffRdd(rdd, other)
  }

  def kvDiff(other: CrRdd[F, K, V]): RDD[KvDiffResult[K, V]] = kvDiff(other.rdd)

  def first(implicit F: SparkDelay[F]): F[Option[OptionalKV[K, V]]] = F.delay(rdd.cminOption)
  def last(implicit F: SparkDelay[F]): F[Option[OptionalKV[K, V]]]  = F.delay(rdd.cmaxOption)

  private def rddResource(implicit F: Sync[F]): Resource[F, RDD[OptionalKV[K, V]]] =
    Resource.make(F.delay(rdd.persist()))(r => F.delay(r.unpersist()))

  // dump java object
  def dump(implicit F: Sync[F], cs: ContextShift[F]): F[Long] = {
    sparkSession.withGroupId(s"nj.rdd.dump.${utils.random4d.value}")

    Blocker[F].use { blocker =>
      val pathStr = params.replayPath(topicName)
      fileSink(blocker).delete(pathStr) >> rddResource.use { data =>
        F.delay(data.saveAsObjectFile(pathStr)).as(data.count())
      }
    }
  }

  //save actions
  def saveJson(blocker: Blocker)(implicit
    F: Sync[F],
    cs: ContextShift[F],
    ek: JsonEncoder[K],
    ev: JsonEncoder[V]): F[Long] = {
    sparkSession.withGroupId(s"nj.rdd.save.circe.json.${utils.random4d.value}")

    val path = params.pathBuilder(topicName, NJFileFormat.Json)

    rddResource.use { data =>
      data
        .stream[F]
        .through(fileSink(blocker).json[OptionalKV[K, V]](path))
        .compile
        .drain
        .as(data.count())
    }
  }

  def saveText(blocker: Blocker)(implicit
    showK: Show[K],
    showV: Show[V],
    F: Sync[F],
    cs: ContextShift[F]): F[Long] = {
    sparkSession.withGroupId(s"nj.rdd.save.text.${utils.random4d.value}")

    val path = params.pathBuilder(topicName, NJFileFormat.Text)

    rddResource.use { data =>
      data
        .stream[F]
        .through(fileSink(blocker).text[OptionalKV[K, V]](path))
        .compile
        .drain
        .as(data.count())
    }
  }

  private def avroLike(blocker: Blocker, fmt: NJFileFormat)(implicit
    F: ConcurrentEffect[F],
    cs: ContextShift[F]): F[Long] = {
    sparkSession.withGroupId(s"nj.rdd.save.${fmt.alias}.${fmt.format}.${utils.random4d.value}")

    val path = params.pathBuilder(topicName, fmt)

    def run(data: RDD[OptionalKV[K, V]]): Stream[F, Unit] =
      fmt match {
        case NJFileFormat.Avro       => data.stream[F].through(fileSink(blocker).avro(path))
        case NJFileFormat.BinaryAvro => data.stream[F].through(fileSink(blocker).binaryAvro(path))
        case NJFileFormat.Jackson    => data.stream[F].through(fileSink(blocker).jackson(path))
        case NJFileFormat.Parquet    => data.stream[F].through(fileSink(blocker).parquet(path))
        case NJFileFormat.JavaObject => data.stream[F].through(fileSink(blocker).javaObject(path))
        case _                       => sys.error("never happen")
      }
    rddResource.use(data => run(data).compile.drain.as(data.count()))
  }

  def saveJackson(
    blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.Jackson)

  def saveAvro(blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.Avro)

  def saveBinaryAvro(
    blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.BinaryAvro)

  def saveParquet(
    blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.Parquet)

  def saveJavaObject(
    blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.JavaObject)

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
