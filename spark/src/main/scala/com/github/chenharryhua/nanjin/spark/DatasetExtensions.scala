package com.github.chenharryhua.nanjin.spark

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync}
import cats.implicits._
import cats.kernel.Eq
import com.sksamuel.avro4s.{Decoder => AvroDecoder, Encoder => AvroEncoder}
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.interop.reactivestreams._
import fs2.{Pipe, Stream}
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.CsvConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag

private[spark] trait DatasetExtensions {

  implicit final class RddExt[A](private val rdd: RDD[A]) {

    def dismissNulls: RDD[A] = rdd.filter(_ != null)
    def numOfNulls: Long     = rdd.subtract(dismissNulls).count()

    def stream[F[_]: Sync]: Stream[F, A] = Stream.fromIterator(rdd.toLocalIterator)

    def source[F[_]: ConcurrentEffect]: Source[A, NotUsed] =
      Source.fromPublisher[A](stream[F].toUnicastPublisher())

    def typedDataset(implicit ev: TypedEncoder[A], ss: SparkSession): TypedDataset[A] =
      TypedDataset.create(rdd)

    def partitionSink[F[_]: Sync, K: ClassTag: Eq](bucketing: A => K)(
      out: K => Pipe[F, A, Unit]): F[Long] = {
      val persisted: RDD[A] = rdd.persist()
      val keys: List[K]     = persisted.map(bucketing).distinct().collect().toList
      keys.traverse(k =>
        persisted.filter(a => k === bucketing(a)).stream[F].through(out(k)).compile.drain) >>
        Sync[F].delay(persisted.count()) <*
        Sync[F].delay(persisted.unpersist())
    }

    def multi[F[_]](blocker: Blocker)(implicit
      ss: SparkSession,
      cs: ContextShift[F],
      F: Sync[F]): RddPersistMultiFile[F, A] =
      new RddPersistMultiFile[F, A](rdd, blocker)

    def single[F[_]](blocker: Blocker)(implicit
      ss: SparkSession,
      cs: ContextShift[F]): RddPersistSingleFile[F, A] =
      new RddPersistSingleFile[F, A](rdd, blocker)

    def toDF(implicit encoder: AvroEncoder[A], ss: SparkSession): DataFrame =
      new RddToDataFrame[A](rdd).toDF
  }

  implicit final class TypedDatasetExt[A](private val tds: TypedDataset[A]) {

    def stream[F[_]: Sync]: Stream[F, A] = tds.dataset.rdd.stream[F]

    def source[F[_]: ConcurrentEffect]: Source[A, NotUsed] =
      Source.fromPublisher[A](stream[F].toUnicastPublisher())

    def dismissNulls: TypedDataset[A]   = tds.deserialized.filter(_ != null)
    def numOfNulls[F[_]: Sync]: F[Long] = tds.except(dismissNulls).count[F]()

    def multi[F[_]](blocker: Blocker)(implicit
      ss: SparkSession,
      cs: ContextShift[F],
      F: Sync[F]): RddPersistMultiFile[F, A] =
      tds.dataset.rdd.multi(blocker)

    def single[F[_]](blocker: Blocker)(implicit
      ss: SparkSession,
      cs: ContextShift[F]): RddPersistSingleFile[F, A] =
      tds.dataset.rdd.single(blocker)
  }

  implicit final class DataframeExt(private val df: DataFrame) {

    def genCaseClass: String = NJDataTypeF.genCaseClass(df.schema)

  }

  implicit final class SparkSessionExt(private val ss: SparkSession) {

    def withGroupId(groupId: String): SparkSession = {
      ss.sparkContext.setLocalProperty("spark.jobGroup.id", groupId)
      ss
    }

    def withDescription(description: String): SparkSession = {
      ss.sparkContext.setLocalProperty("spark.job.description", description)
      ss
    }

    private val delegate: SparkReadFile = new SparkReadFile(ss)

    def parquet[A: TypedEncoder](pathStr: String): TypedDataset[A] =
      delegate.parquet[A](pathStr)

    def avro[A: ClassTag](pathStr: String)(implicit decoder: AvroDecoder[A]): RDD[A] =
      delegate.avro[A](pathStr)

    def circe[A: ClassTag: JsonDecoder](pathStr: String): RDD[A] =
      delegate.circe[A](pathStr)

    def jackson[A: ClassTag](pathStr: String)(implicit decoder: AvroDecoder[A]): RDD[A] =
      delegate.jackson[A](pathStr)

    def csv[A: ClassTag: TypedEncoder](
      pathStr: String,
      csvConfig: CsvConfiguration): TypedDataset[A] =
      delegate.csv(pathStr, csvConfig)

    def csv[A: ClassTag: TypedEncoder](pathStr: String): TypedDataset[A] =
      csv[A](pathStr, CsvConfiguration.rfc)

    def text(pathStr: String): TypedDataset[String] =
      delegate.text(pathStr)
  }
}
