package com.github.chenharryhua.nanjin

import cats.effect.kernel.{Resource, Sync}
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.kafka.KafkaContext
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.persist.*
import com.github.chenharryhua.nanjin.spark.table.LoadTable
import com.github.chenharryhua.nanjin.terminals.Hadoop
import com.sksamuel.avro4s.Encoder as AvroEncoder
import com.zaxxer.hikari.HikariConfig
import fs2.Stream
import io.lemonlabs.uri.Url
import org.apache.avro.Schema
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.reflect.ClassTag

package object spark {
  object injection extends InjectionInstances

  def describeJob[F[_]](sparkContext: SparkContext, description: String)(implicit
    F: Sync[F]): Resource[F, Unit] =
    Resource.make(F.delay(sparkContext.setJobDescription(description)))(_ =>
      F.delay(sparkContext.setJobDescription(null)))

  implicit final class RddExt[A](rdd: RDD[A]) extends Serializable {

    def output: RddFileHoarder[A] = new RddFileHoarder[A](rdd)

    def output(encoder: AvroEncoder[A]): RddAvroFileHoarder[A] =
      new RddAvroFileHoarder[A](rdd, encoder)

    def stream[F[_]: Sync](chunkSize: ChunkSize): Stream[F, A] =
      Stream.fromBlockingIterator[F](rdd.toLocalIterator, chunkSize.value)
  }

  implicit final class DataframeExt(df: DataFrame) extends Serializable {

    def genCaseClass: String = NJDataType(df.schema).toCaseClass
    def genSchema: Schema = NJDataType(df.schema).toSchema
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

    def loadTable[A](ate: SchematizedEncoder[A]): LoadTable[A] = new LoadTable[A](ate, ss)
    def loadRdd[A: ClassTag](path: Url): LoadRdd[A] = new LoadRdd[A](ss, path)
    def loadProtobuf[A <: GeneratedMessage: ClassTag: GeneratedMessageCompanion](path: Url): RDD[A] =
      loaders.rdd.protobuf[A](path, ss)

    def alongWith[F[_]](ctx: KafkaContext[F]): SparKafkaContext[F] =
      new SparKafkaContext[F](ss, ctx)

    def hadoop[F[_]]: Hadoop[F] = Hadoop[F](ss.sparkContext.hadoopConfiguration)

  }

  def structType[A](avroCodec: AvroCodec[A]): StructType =
    SchemaConverters.toSqlType(avroCodec.schema).dataType match {
      case st: StructType => st
      case primitive      => StructType(Array(StructField("value", primitive)))
    }
}
