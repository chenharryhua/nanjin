package com.github.chenharryhua.nanjin

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaTopic}
import com.github.chenharryhua.nanjin.spark.kafka.SparKafkaTopic
import com.github.chenharryhua.nanjin.spark.persist.*
import com.github.chenharryhua.nanjin.spark.table.LoadTable
import com.github.chenharryhua.nanjin.terminals.NJHadoop
import com.sksamuel.avro4s.Encoder as AvroEncoder
import com.zaxxer.hikari.HikariConfig
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.DataType

package object spark {
  final val SPARK_ZONE_ID: String = "spark.sql.session.timeZone"

  object injection extends InjectionInstances

  implicit final class RddExt[F[_], A](frdd: F[RDD[A]]) extends Serializable {

    def output: RddFileHoarder[F, A] = new RddFileHoarder[F, A](frdd)

    def output(encoder: AvroEncoder[A]): RddAvroFileHoarder[F, A] =
      new RddAvroFileHoarder[F, A](frdd, encoder)
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

    final class PartialApplyAvroTypedEncoder[F[_]] {
      def apply[A](ate: AvroTypedEncoder[A])(implicit F: Sync[F]) = new LoadTable[F, A](ate, ss)
    }

    def loadTable[F[_]]: PartialApplyAvroTypedEncoder[F] = new PartialApplyAvroTypedEncoder[F]

    def alongWith[F[_]](ctx: KafkaContext[F]): SparKafkaContext[F] =
      new SparKafkaContext[F](ss, ctx)

    def topic[F[_], K, V](topic: KafkaTopic[F, K, V]): SparKafkaTopic[F, K, V] =
      new SparKafkaContext[F](ss, topic.context).topic(topic)

    def hadoop[F[_]]: NJHadoop[F] = NJHadoop[F](ss.sparkContext.hadoopConfiguration)

  }
}
