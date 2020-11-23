package com.github.chenharryhua.nanjin.spark

import java.time.ZoneId

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.effect.{ConcurrentEffect, Sync}
import com.github.chenharryhua.nanjin.database.{DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, TopicDef}
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.database.{sd, DbUploader, SparkTable}
import com.github.chenharryhua.nanjin.spark.kafka.{SKConfig, SparKafka}
import com.github.chenharryhua.nanjin.spark.persist.RddFileHoarder
import frameless.TypedDataset
import frameless.cats.implicits._
import fs2.Stream
import fs2.interop.reactivestreams._
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

private[spark] trait DatasetExtensions {

  implicit final class RddExt[A](rdd: RDD[A]) extends Serializable {

    def dismissNulls: RDD[A] = rdd.filter(_ != null)
    def numOfNulls: Long     = rdd.subtract(dismissNulls).count()

    def stream[F[_]: Sync]: Stream[F, A] = Stream.fromIterator(rdd.toLocalIterator)

    def source[F[_]: ConcurrentEffect]: Source[A, NotUsed] =
      Source.fromPublisher[A](stream[F].toUnicastPublisher)

    def typedDataset(ate: AvroTypedEncoder[A])(implicit ss: SparkSession): TypedDataset[A] =
      ate.normalize(rdd)(ss)

    def dbUpload[F[_]: Sync](db: SparkTable[F, A]): DbUploader[F, A] =
      db.tableset(rdd).upload

    def save[F[_]](codec: AvroCodec[A]): RddFileHoarder[F, A] =
      new RddFileHoarder[F, A](rdd, codec)
  }

  implicit final class TypedDatasetExt[A](tds: TypedDataset[A]) extends Serializable {

    def stream[F[_]: Sync]: Stream[F, A] = tds.dataset.rdd.stream[F]

    def source[F[_]: ConcurrentEffect]: Source[A, NotUsed] =
      Source.fromPublisher[A](stream[F].toUnicastPublisher)

    def dismissNulls: TypedDataset[A]   = tds.deserialized.filter(_ != null)
    def numOfNulls[F[_]: Sync]: F[Long] = tds.except(dismissNulls).count[F]()

    def dbUpload[F[_]: Sync](db: SparkTable[F, A]): DbUploader[F, A] =
      db.tableset(tds).upload

    def save[F[_]](ate: AvroTypedEncoder[A]): RddFileHoarder[F, A] =
      new RddFileHoarder[F, A](tds.dataset.rdd, ate.avroCodec)
  }

  implicit final class DataframeExt(df: DataFrame) extends Serializable {

    def genCaseClass: String = NJDataType(df.schema).toCaseClass
    def genSchema: Schema    = NJDataType(df.schema).toSchema

  }

  final class SparkWithDBSettings[F[_]](ss: SparkSession, dbSettings: DatabaseSettings)
      extends Serializable {

    def dataframe(tableName: String): DataFrame =
      sd.unloadDF(dbSettings.hikariConfig, TableName.unsafeFrom(tableName), None)(ss)

    def genCaseClass(tableName: String): String = dataframe(tableName).genCaseClass
    def genSchema(tableName: String): Schema    = dataframe(tableName).genSchema

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
