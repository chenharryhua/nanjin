package com.github.chenharryhua.nanjin

import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaTopic}
import com.github.chenharryhua.nanjin.spark.kafka.SparKafkaTopic
import com.github.chenharryhua.nanjin.spark.persist.*
import com.github.chenharryhua.nanjin.spark.table.LoadTable
import com.github.chenharryhua.nanjin.terminals.Hadoop
import com.sksamuel.avro4s.{Decoder, Encoder as AvroEncoder}
import com.zaxxer.hikari.HikariConfig
import io.lemonlabs.uri.Url
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, SparkSession}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.reflect.ClassTag

package object spark {
  object injection extends InjectionInstances

  implicit final class RddExt[F[_], A](rdd: RDD[A]) extends Serializable {

    def output: RddFileHoarder[A] = new RddFileHoarder[A](rdd)

    def output(encoder: AvroEncoder[A]): RddAvroFileHoarder[A] =
      new RddAvroFileHoarder[A](rdd, encoder)
  }

  implicit final class DataframeExt(df: DataFrame) extends Serializable {

    def genCaseClass: String  = NJDataType(df.schema).toCaseClass
    def genSchema: Schema     = NJDataType(df.schema).toSchema
    def genDataType: DataType = NJDataType(df.schema).toSpark

  }

  implicit final class SparkSessionExt(ss: SparkSession) extends Serializable {

    /** @param dbtable
      *   schema.table
      * @return
      */
    def jdbcDataFrame(hikari: HikariConfig, dbtable: String): DataFrame = {
      val options: Map[String, String] = Map(
        "url" -> hikari.getJdbcUrl,
        "driver" -> hikari.getDriverClassName,
        "user" -> hikari.getUsername,
        "password" -> hikari.getPassword,
        "dbtable" -> dbtable)
      ss.read.format("jdbc").options(options).load()
    }

    def loadTable[A](ate: AvroTypedEncoder[A]): LoadTable[A] = new LoadTable[A](ate, ss)
    def loadRDD[A: ClassTag](path: Url): LoadRDD[A]          = new LoadRDD[A](ss, path)
    def loadAvro[A: ClassTag: Decoder](path: Url): LoadAvroRDD[A] =
      new LoadAvroRDD[A](ss, path, Decoder[A])
    def loadProtobuf[A <: GeneratedMessage: ClassTag: GeneratedMessageCompanion](path: Url): RDD[A] =
      loaders.rdd.protobuf[A](path, ss)

    def alongWith[F[_]](ctx: KafkaContext[F]): SparKafkaContext[F] =
      new SparKafkaContext[F](ss, ctx)

    def topic[F[_], K, V](topic: KafkaTopic[F, K, V]): SparKafkaTopic[F, K, V] =
      new SparKafkaContext[F](ss, KafkaContext[F](topic.settings)).topic(topic.topicDef)

    def hadoop[F[_]]: Hadoop[F] = Hadoop[F](ss.sparkContext.hadoopConfiguration)

  }
}
