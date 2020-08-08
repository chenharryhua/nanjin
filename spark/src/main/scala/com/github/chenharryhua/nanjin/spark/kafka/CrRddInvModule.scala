package com.github.chenharryhua.nanjin.spark.kafka

import cats.Eq
import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.pipes.{GenericRecordEncoder, JacksonSerialization}
import com.sksamuel.avro4s.{AvroSchema, SchemaFor, Encoder => AvroEncoder}
import frameless.cats.implicits.rddOps
import frameless.{SparkDelay, TypedDataset}
import fs2.Stream
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Queue

trait CrRddInvModule[F[_], K, V] { self: CrRdd[F, K, V] =>

  def count(implicit F: SparkDelay[F]): F[Long] =
    F.delay(rdd.count())

  def stats: Statistics[F] =
    new Statistics[F](TypedDataset.create(rdd.map(CRMetaInfo(_))).dataset, cfg)

  def malOrder(implicit F: Sync[F]): Stream[F, OptionalKV[K, V]] =
    stream.sliding(2).mapFilter {
      case Queue(a, b) => if (a.timestamp <= b.timestamp) None else Some(a)
    }

  def missingData: TypedDataset[CRMetaInfo] =
    inv.missingData(TypedDataset.create(values.map(CRMetaInfo(_))))

  def dupRecords: TypedDataset[DupResult] =
    inv.dupRecords(TypedDataset.create(values.map(CRMetaInfo(_))))

  def showJackson(rs: Array[OptionalKV[K, V]])(implicit F: Sync[F]): F[Unit] = {
    implicit val ks: SchemaFor[K] = keyAvroEncoder.schemaFor
    implicit val vs: SchemaFor[V] = valAvroEncoder.schemaFor

    val pipe: JacksonSerialization[F] = new JacksonSerialization[F](AvroSchema[OptionalKV[K, V]])
    val gre: GenericRecordEncoder[F, OptionalKV[K, V]] =
      new GenericRecordEncoder[F, OptionalKV[K, V]](AvroEncoder[OptionalKV[K, V]])

    Stream
      .emits(rs)
      .covary[F]
      .through(gre.encode)
      .through(pipe.compactJson)
      .showLinesStdOut
      .compile
      .drain
  }

  def find(f: OptionalKV[K, V] => Boolean)(implicit F: Sync[F]): F[Unit] =
    showJackson(rdd.filter(f).take(params.showDs.rowNum))

  def show(implicit F: Sync[F]): F[Unit] =
    showJackson(rdd.take(params.showDs.rowNum))

  def diff(other: RDD[OptionalKV[K, V]])(implicit ek: Eq[K], ev: Eq[V]): RDD[DiffResult[K, V]] =
    inv.diffRdd(rdd, other)

  def diff(other: CrRdd[F, K, V])(implicit ek: Eq[K], ev: Eq[V]): RDD[DiffResult[K, V]] =
    diff(other.rdd)

  def kvDiff(other: RDD[OptionalKV[K, V]]): RDD[KvDiffResult[K, V]] =
    inv.kvDiffRdd(rdd, other)

  def kvDiff(other: CrRdd[F, K, V]): RDD[KvDiffResult[K, V]] = kvDiff(other.rdd)

  def first(implicit F: SparkDelay[F]): F[Option[OptionalKV[K, V]]] = F.delay(rdd.cminOption)
  def last(implicit F: SparkDelay[F]): F[Option[OptionalKV[K, V]]]  = F.delay(rdd.cmaxOption)

}
