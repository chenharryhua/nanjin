package com.github.chenharryhua.nanjin

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.database.DatabaseSettings
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaTopic}
import com.github.chenharryhua.nanjin.spark.database.{DbUploader, SparkDBTable}
import com.github.chenharryhua.nanjin.spark.kafka.SparKafkaTopic
import com.github.chenharryhua.nanjin.spark.persist.{
  DatasetAvroFileHoarder,
  DatasetFileHoarder,
  RddAvroFileHoarder,
  RddFileHoarder
}
import com.sksamuel.avro4s.Encoder as AvroEncoder
import frameless.TypedDataset
import fs2.Stream
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag

package object spark {

  object injection extends InjectionInstances

  private[spark] val SparkDatetimeConversionConstant: Int = 1000

  implicit final class RddExt[A](rdd: RDD[A]) extends Serializable {

    def dismissNulls(implicit ev: ClassTag[A]): RDD[A] = rdd.flatMap(Option(_))
    def numOfNulls(implicit ev: ClassTag[A]): Long     = rdd.subtract(dismissNulls).count()

    def stream[F[_]: Sync](chunkSize: Int): Stream[F, A] = Stream.fromIterator(rdd.toLocalIterator, chunkSize)

    def dbUpload[F[_]: Sync](db: SparkDBTable[F, A]): DbUploader[F, A] =
      db.tableset(rdd).upload

    def save[F[_]]: RddFileHoarder[F, A] = new RddFileHoarder[F, A](rdd)

    def save[F[_]](encoder: AvroEncoder[A]): RddAvroFileHoarder[F, A] =
      new RddAvroFileHoarder[F, A](rdd, encoder)

  }

  implicit final class TypedDatasetExt[A](tds: TypedDataset[A]) extends Serializable {

    def stream[F[_]: Sync](chunkSize: Int): Stream[F, A] = tds.rdd.stream[F](chunkSize)

    def dismissNulls: TypedDataset[A] = tds.deserialized.flatMap(Option(_))(tds.encoder)
    def numOfNulls: Long              = tds.except(dismissNulls).dataset.count()

    def dbUpload[F[_]: Sync](db: SparkDBTable[F, A]): DbUploader[F, A] = db.tableset(tds).upload

    def save[F[_]]: DatasetFileHoarder[F, A] = new DatasetFileHoarder[F, A](tds.dataset)

    def save[F[_]](encoder: AvroEncoder[A]): DatasetAvroFileHoarder[F, A] =
      new DatasetAvroFileHoarder[F, A](tds.dataset, encoder)
  }

  implicit final class DataframeExt(df: DataFrame) extends Serializable {

    def genCaseClass: String  = NJDataType(df.schema).toCaseClass
    def genSchema: Schema     = NJDataType(df.schema).toSchema
    def genDataType: DataType = NJDataType(df.schema).toSpark

  }

  implicit final class SparkSessionExt(ss: SparkSession) extends Serializable {

    def alongWith[F[_]](dbSettings: DatabaseSettings): SparkDBContext[F] =
      new SparkDBContext[F](ss, dbSettings)

    def alongWith[F[_]](ctx: KafkaContext[F]): SparKafkaContext[F] =
      new SparKafkaContext[F](ss, ctx)

    def topic[F[_], K, V](topic: KafkaTopic[F, K, V]): SparKafkaTopic[F, K, V] =
      new SparKafkaContext[F](ss, topic.context).topic(topic)

  }
}
