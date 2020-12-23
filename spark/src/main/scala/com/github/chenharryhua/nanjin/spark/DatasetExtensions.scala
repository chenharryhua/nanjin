package com.github.chenharryhua.nanjin.spark

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.effect.{ConcurrentEffect, Sync}
import com.github.chenharryhua.nanjin.database.{DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, TopicDef}
import com.github.chenharryhua.nanjin.spark.database._
import com.github.chenharryhua.nanjin.spark.kafka.{SKConfig, SparKafka}
import com.github.chenharryhua.nanjin.spark.persist.{
  DatasetAvroFileHoarder,
  DatasetFileHoarder,
  RddAvroFileHoarder,
  RddFileHoarder
}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.TypedDataset
import frameless.cats.implicits._
import fs2.Stream
import fs2.interop.reactivestreams._
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.ZoneId

private[spark] trait DatasetExtensions {

  implicit final class RddExt[A](rdd: RDD[A]) extends Serializable {

    def dismissNulls: RDD[A] = rdd.filter(_ != null)
    def numOfNulls: Long     = rdd.subtract(dismissNulls).count()

    def stream[F[_]: Sync]: Stream[F, A] = Stream.fromIterator(rdd.toLocalIterator)

    def source[F[_]: ConcurrentEffect]: Source[A, NotUsed] =
      Source.fromPublisher[A](stream[F].toUnicastPublisher)

    def typedDataset(ate: AvroTypedEncoder[A])(implicit ss: SparkSession): TypedDataset[A] =
      ate.normalize(rdd, ss)

    def dbUpload[F[_]: Sync](db: SparkTable[F, A]): DbUploader[F, A] =
      db.tableset(rdd).upload

    def save[F[_]]: RddFileHoarder[F, A] = new RddFileHoarder[F, A](rdd)

    def save[F[_]](encoder: AvroEncoder[A]): RddAvroFileHoarder[F, A] =
      new RddAvroFileHoarder[F, A](rdd, encoder)

  }

  implicit final class TypedDatasetExt[A](tds: TypedDataset[A]) extends Serializable {

    def stream[F[_]: Sync]: Stream[F, A]                   = tds.rdd.stream[F]
    def source[F[_]: ConcurrentEffect]: Source[A, NotUsed] = tds.rdd.source[F]

    def dismissNulls: TypedDataset[A]   = tds.deserialized.filter(_ != null)
    def numOfNulls[F[_]: Sync]: F[Long] = tds.except(dismissNulls).count[F]()

    def dbUpload[F[_]: Sync](db: SparkTable[F, A]): DbUploader[F, A] =
      tds.rdd.dbUpload(db)

    def save[F[_]]: DatasetFileHoarder[F, A] = new DatasetFileHoarder[F, A](tds.dataset)

    def save[F[_]](encoder: AvroEncoder[A]): DatasetAvroFileHoarder[F, A] =
      new DatasetAvroFileHoarder[F, A](tds.dataset, encoder)
  }

  implicit final class DataframeExt(df: DataFrame) extends Serializable {

    def genCaseClass: String  = NJDataType(df.schema).toCaseClass
    def genSchema: Schema     = NJDataType(df.schema).toSchema
    def genDataType: DataType = NJDataType(df.schema).toSpark

  }

  final class SparkWithDBSettings[F[_]](ss: SparkSession, dbSettings: DatabaseSettings)
      extends Serializable {

    def dataframe(tableName: String): DataFrame =
      sd.unloadDF(dbSettings.hikariConfig, TableName.unsafeFrom(tableName), None)(ss)

    def genCaseClass(tableName: String): String  = dataframe(tableName).genCaseClass
    def genSchema(tableName: String): Schema     = dataframe(tableName).genSchema
    def genDatatype(tableName: String): DataType = dataframe(tableName).genDataType

    def table[A](tableDef: TableDef[A]): SparkTable[F, A] = {
      val cfg = STConfig(dbSettings.database, tableDef.tableName)
      new SparkTable[F, A](tableDef, dbSettings, cfg)(ss)
    }
  }

  final class SparkWithKafkaContext[F[_]](ss: SparkSession, ctx: KafkaContext[F])
      extends Serializable {

    def topic[K, V](topicDef: TopicDef[K, V]): SparKafka[F, K, V] =
      new SparKafka[F, K, V](
        topicDef.in[F](ctx),
        ss,
        SKConfig(topicDef.topicName, ZoneId.systemDefault())
      )
  }

  implicit final class SparkSessionExt(ss: SparkSession) extends Serializable {

    def alongWith[F[_]](dbSettings: DatabaseSettings): SparkWithDBSettings[F] =
      new SparkWithDBSettings[F](ss, dbSettings)

    def alongWith[F[_]](ctx: KafkaContext[F]): SparkWithKafkaContext[F] =
      new SparkWithKafkaContext[F](ss, ctx)

  }
}
