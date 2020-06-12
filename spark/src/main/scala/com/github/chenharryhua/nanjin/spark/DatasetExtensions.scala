package com.github.chenharryhua.nanjin.spark

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.Order
import cats.effect.{ConcurrentEffect, Sync}
import cats.implicits._
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.interop.reactivestreams._
import fs2.{Pipe, Stream}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag

private[spark] trait DatasetExtensions {

  implicit class RddExt[A](private val rdd: RDD[A]) {
    def validRecords(implicit tag: ClassTag[A]): RDD[A]   = rdd.flatMap(Option(_))
    def invalidRecords(implicit tag: ClassTag[A]): RDD[A] = rdd.subtract(validRecords)

    def stream[F[_]: Sync]: Stream[F, A] =
      Stream.fromIterator(rdd.toLocalIterator.flatMap(Option(_)))

    def source[F[_]: ConcurrentEffect]: Source[A, NotUsed] =
      Source.fromPublisher[A](stream[F].toUnicastPublisher())

    def typedDataset(implicit ev: TypedEncoder[A], ss: SparkSession): TypedDataset[A] =
      TypedDataset.create(rdd)

    def partitionSink[F[_]: Sync, K: ClassTag: Order](bucketing: A => K)(
      out: K => Pipe[F, A, Unit]): F[Unit] = {
      val persisted: RDD[A] = rdd.persist()
      val keys: List[K]     = persisted.map(bucketing).distinct().collect().toList.sorted
      keys
        .map(k => persisted.filter(a => k === bucketing(a)).stream[F].through(out(k)).compile.drain)
        .reduce(_ >> _) >> Sync[F].delay(persisted.unpersist())
    }
  }

  implicit class TypedDatasetExt[A](private val tds: TypedDataset[A]) {

    def stream[F[_]: Sync]: Stream[F, A] = tds.dataset.rdd.stream[F]

    def source[F[_]: ConcurrentEffect]: Source[A, NotUsed] =
      Source.fromPublisher[A](stream[F].toUnicastPublisher())

    def validRecords: TypedDataset[A] = {
      import tds.encoder
      tds.deserialized.flatMap(Option(_))
    }

    def invalidRecords: TypedDataset[A] =
      tds.except(validRecords)
  }

  implicit class DataframeExt(private val df: DataFrame) {

    def genCaseClass: String = NJDataTypeF.genCaseClass(df.schema)

  }

  implicit class SparkSessionExt(private val ss: SparkSession) {

    def parquet[A: TypedEncoder](pathStr: String): TypedDataset[A] =
      TypedDataset.createUnsafe[A](ss.read.parquet(pathStr))

    def avro[A: TypedEncoder](pathStr: String): TypedDataset[A] =
      TypedDataset.createUnsafe(ss.read.format("avro").load(pathStr))

    def text(path: String): TypedDataset[String] =
      TypedDataset.create(ss.read.textFile(path))
  }
}
