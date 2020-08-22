package com.github.chenharryhua.nanjin.spark

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.effect.{ConcurrentEffect, Sync}
import com.github.chenharryhua.nanjin.database.{DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.spark.database.sd
import com.github.chenharryhua.nanjin.spark.saver.{RddFileLoader, RddFileSaver}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import fs2.interop.reactivestreams._
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.{DataFrame, SparkSession}

private[spark] trait DatasetExtensions {

  implicit final class RddExt[A](private val rdd: RDD[A]) {

    def dismissNulls: RDD[A] = rdd.filter(_ != null)
    def numOfNulls: Long     = rdd.subtract(dismissNulls).count()

    def stream[F[_]: Sync]: Stream[F, A] = Stream.fromIterator(rdd.toLocalIterator)

    def source[F[_]: ConcurrentEffect]: Source[A, NotUsed] =
      Source.fromPublisher[A](stream[F].toUnicastPublisher)

    def typedDataset(implicit ev: TypedEncoder[A], ss: SparkSession): TypedDataset[A] =
      TypedDataset.create(rdd)

    def toDF(implicit encoder: AvroEncoder[A], ss: SparkSession): DataFrame =
      utils.rddToDataFrame[A](rdd, encoder, ss)

    def save[F[_]]: RddFileSaver[F, A] = new RddFileSaver[F, A](rdd)
  }

  implicit final class TypedDatasetExt[A](private val tds: TypedDataset[A]) {

    def stream[F[_]: Sync]: Stream[F, A] = tds.dataset.rdd.stream[F]

    def source[F[_]: ConcurrentEffect]: Source[A, NotUsed] =
      Source.fromPublisher[A](stream[F].toUnicastPublisher)

    def dismissNulls: TypedDataset[A]   = tds.deserialized.filter(_ != null)
    def numOfNulls[F[_]: Sync]: F[Long] = tds.except(dismissNulls).count[F]()

    def save[F[_]]: RddFileSaver[F, A] =
      new RddFileSaver[F, A](tds.dataset.rdd)

  }

  implicit final class DataframeExt(private val df: DataFrame) {

    def genCaseClass: String = NJDataTypeF.genCaseClass(df.schema)
    def genSchema: Schema    = NJDataTypeF.genSchema(df.schema)

  }

  final class SparkWithDBSettings(ss: SparkSession, dbSettings: DatabaseSettings) {

    def dataframe(tableName: String): DataFrame =
      sd.unloadDF(dbSettings.connStr, dbSettings.driver, TableName.unsafeFrom(tableName), None)(ss)

    def schema(tableName: String): Schema =
      dataframe(tableName).genSchema

  }

  implicit final class SparkSessionExt(private val ss: SparkSession) {

    val load: RddFileLoader = new RddFileLoader(ss)

    def alongWith(dbSettings: DatabaseSettings): SparkWithDBSettings =
      new SparkWithDBSettings(ss, dbSettings)
  }
}
