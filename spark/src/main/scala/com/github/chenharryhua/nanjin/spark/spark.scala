package com.github.chenharryhua.nanjin

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaTopic}
import com.github.chenharryhua.nanjin.spark.kafka.SparKafkaTopic
import com.github.chenharryhua.nanjin.spark.persist.*
import com.github.chenharryhua.nanjin.spark.table.LoadTable
import com.github.chenharryhua.nanjin.terminals.NJHadoop
import com.sksamuel.avro4s.Encoder as AvroEncoder
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

package object spark {

  object injection extends InjectionInstances

  private[spark] val SparkDatetimeConversionConstant: Int = 1000

  implicit final class RddExt[A](rdd: RDD[A]) extends Serializable {

    def dismissNulls(implicit ev: ClassTag[A]): RDD[A] = rdd.flatMap(Option(_))
    def numOfNulls(implicit ev: ClassTag[A]): Long     = rdd.subtract(dismissNulls).count()

    def save[F[_]]: RddFileHoarder[F, A] = new RddFileHoarder[F, A](rdd)

    def save[F[_]](encoder: AvroEncoder[A]): RddAvroFileHoarder[F, A] =
      new RddAvroFileHoarder[F, A](rdd, encoder)

    def asSource[F[_]]: RddStreamSource[F, A] = new RddStreamSource[F, A](rdd)

  }

  implicit final class DatasetExt[A](ds: Dataset[A]) extends Serializable {

    def dismissNulls: Dataset[A] = ds.flatMap(Option(_))(ds.encoder)
    def numOfNulls: Long         = ds.except(dismissNulls).count()

    def asSource[F[_]]: RddStreamSource[F, A] = new RddStreamSource[F, A](ds.rdd)

  }

  implicit final class DataframeExt(df: DataFrame) extends Serializable {

    def genCaseClass: String  = NJDataType(df.schema).toCaseClass
    def genSchema: Schema     = NJDataType(df.schema).toSchema
    def genDataType: DataType = NJDataType(df.schema).toSpark

  }

  implicit final class SparkSessionExt(ss: SparkSession) extends Serializable {

    def loadWith[A](ate: AvroTypedEncoder[A]): LoadTable[A] = new LoadTable[A](ate, ss)

    def alongWith[F[_]](ctx: KafkaContext[F]): SparKafkaContext[F] =
      new SparKafkaContext[F](ss, ctx)

    def topic[F[_], K, V](topic: KafkaTopic[F, K, V]): SparKafkaTopic[F, K, V] =
      new SparKafkaContext[F](ss, topic.context).topic(topic)

    def hadoop[F[_]: Sync]: NJHadoop[F] = NJHadoop[F](ss.sparkContext.hadoopConfiguration)

  }
}
