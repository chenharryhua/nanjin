package com.github.chenharryhua.nanjin.spark

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.database.DatabaseSettings
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaTopic, TopicDef}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{KJson, SerdeOf}
import com.github.chenharryhua.nanjin.pipes.chunkSize
import com.github.chenharryhua.nanjin.spark.database.*
import com.github.chenharryhua.nanjin.spark.kafka.{SKConfig, SparKafkaTopic}
import com.github.chenharryhua.nanjin.spark.persist.{
  DatasetAvroFileHoarder,
  DatasetFileHoarder,
  RddAvroFileHoarder,
  RddFileHoarder
}
import com.sksamuel.avro4s.{SchemaFor, Decoder as AvroDecoder, Encoder as AvroEncoder}
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import io.circe.Json
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.ZoneId

private[spark] trait DatasetExtensions {

  implicit final class RddExt[A](rdd: RDD[A]) extends Serializable {

    def dismissNulls: RDD[A] = rdd.filter(_ != null)
    def numOfNulls: Long     = rdd.subtract(dismissNulls).count()

    def stream[F[_]: Sync]: Stream[F, A] = Stream.fromIterator(rdd.toLocalIterator, chunkSize)

    def dbUpload[F[_]: Sync](db: SparkDBTable[F, A]): DbUploader[F, A] =
      db.tableset(rdd).upload

    def save[F[_]]: RddFileHoarder[F, A] = new RddFileHoarder[F, A](rdd)

    def save[F[_]](encoder: AvroEncoder[A]): RddAvroFileHoarder[F, A] =
      new RddAvroFileHoarder[F, A](rdd, encoder)

  }

  implicit final class TypedDatasetExt[A](tds: TypedDataset[A]) extends Serializable {

    def stream[F[_]: Sync]: Stream[F, A] = tds.rdd.stream[F]

    def dismissNulls: TypedDataset[A] = tds.deserialized.filter(_ != null)
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

final class SparkDBContext[F[_]](val sparkSession: SparkSession, val dbSettings: DatabaseSettings)
    extends Serializable {

  def dataframe(tableName: String): DataFrame =
    sd.unloadDF(dbSettings.hikariConfig, TableName.unsafeFrom(tableName), None, sparkSession)

  def genCaseClass(tableName: String): String  = dataframe(tableName).genCaseClass
  def genSchema(tableName: String): Schema     = dataframe(tableName).genSchema
  def genDatatype(tableName: String): DataType = dataframe(tableName).genDataType

  def table[A](tableDef: TableDef[A]): SparkDBTable[F, A] = {
    val cfg = STConfig(dbSettings.database, tableDef.tableName)
    new SparkDBTable[F, A](tableDef, dbSettings, cfg, sparkSession)
  }

  def table[A: AvroEncoder: AvroDecoder: SchemaFor: TypedEncoder](tableName: String): SparkDBTable[F, A] =
    table[A](TableDef[A](TableName.unsafeFrom(tableName)))

}

final class SparKafkaContext[F[_]](val sparkSession: SparkSession, val kafkaContext: KafkaContext[F])
    extends Serializable {

  def topic[K, V](topicDef: TopicDef[K, V]): SparKafkaTopic[F, K, V] = {
    val zoneId = ZoneId.of(sparkSession.conf.get("spark.sql.session.timeZone"))
    new SparKafkaTopic[F, K, V](topicDef.in[F](kafkaContext), SKConfig(topicDef.topicName, zoneId), sparkSession)
  }

  def topic[K, V](kt: KafkaTopic[F, K, V]): SparKafkaTopic[F, K, V] =
    topic[K, V](kt.topicDef)

  def topic[K: SerdeOf, V: SerdeOf](topicName: String): SparKafkaTopic[F, K, V] =
    topic[K, V](TopicDef[K, V](TopicName.unsafeFrom(topicName)))

  def byteTopic(topicName: String): SparKafkaTopic[F, Array[Byte], Array[Byte]] =
    topic[Array[Byte], Array[Byte]](topicName)

  def stringTopic(topicName: String): SparKafkaTopic[F, String, String] =
    topic[String, String](topicName)

  def jsonTopic(topicName: String): SparKafkaTopic[F, KJson[Json], KJson[Json]] =
    topic[KJson[Json], KJson[Json]](topicName)
}
