package com.github.chenharryhua.nanjin.spark.persist

import cats.{Eq, Show}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.common.NJFileFormat._
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import frameless.TypedEncoder
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scalapb.GeneratedMessage

import scala.reflect.ClassTag

class RddPartitionHoarder[F[_], A: ClassTag, K: Eq: ClassTag](
  rdd: RDD[A],
  codec: AvroCodec[A],
  bucketing: A => Option[K],
  pathBuilder: (NJFileFormat, K) => String,
  cfg: HoarderConfig = HoarderConfig.default)
    extends Serializable {

  private def updateConfig(cfg: HoarderConfig): RddPartitionHoarder[F, A, K] =
    new RddPartitionHoarder[F, A, K](rdd, codec, bucketing, pathBuilder, cfg)

  def errorIfExists: RddPartitionHoarder[F, A, K]  = updateConfig(cfg.withError)
  def overwrite: RddPartitionHoarder[F, A, K]      = updateConfig(cfg.withOverwrite)
  def ignoreIfExists: RddPartitionHoarder[F, A, K] = updateConfig(cfg.withIgnore)

  def file: RddPartitionHoarder[F, A, K]   = updateConfig(cfg.withSingleFile)
  def folder: RddPartitionHoarder[F, A, K] = updateConfig(cfg.withFolder)

  def parallel(num: Long): RddPartitionHoarder[F, A, K] =
    updateConfig(cfg.withParallel(num))

  def reBucket[K1: ClassTag: Eq](
    bucketing: A => Option[K1],
    pathBuilder: (NJFileFormat, K1) => String): RddPartitionHoarder[F, A, K1] =
    new RddPartitionHoarder[F, A, K1](rdd, codec, bucketing, pathBuilder, cfg)

  def rePath(pathBuilder: (NJFileFormat, K) => String): RddPartitionHoarder[F, A, K] =
    new RddPartitionHoarder[F, A, K](rdd, codec, bucketing, pathBuilder, cfg)

// 1
  def jackson: PartitionJackson[F, A, K] =
    new PartitionJackson[F, A, K](rdd, codec, cfg.withFormat(Jackson), bucketing, pathBuilder)

// 2
  def circe(implicit ev: JsonEncoder[A]): PartitionCirce[F, A, K] =
    new PartitionCirce[F, A, K](rdd, codec, cfg.withFormat(Circe), bucketing, pathBuilder)

// 3
  def text(implicit ev: Show[A]): PartitionText[F, A, K] =
    new PartitionText[F, A, K](rdd, codec, cfg.withFormat(Text), bucketing, pathBuilder)

// 4
  def csv(implicit ev: RowEncoder[A], te: TypedEncoder[A]): PartitionCsv[F, A, K] = {
    val ate: AvroTypedEncoder[A] = AvroTypedEncoder[A](te, codec)
    new PartitionCsv[F, A, K](
      rdd,
      ate,
      CsvConfiguration.rfc,
      cfg.withFormat(Csv),
      bucketing,
      pathBuilder)
  }

  // 5
  def json(implicit te: TypedEncoder[A]): PartitionSparkJson[F, A, K] = {
    val ate: AvroTypedEncoder[A] = AvroTypedEncoder[A](te, codec)
    new PartitionSparkJson[F, A, K](rdd, ate, cfg.withFormat(SparkJson), bucketing, pathBuilder)
  }

  // 11
  def parquet(implicit te: TypedEncoder[A]): PartitionParquet[F, A, K] = {
    val ate: AvroTypedEncoder[A] = AvroTypedEncoder[A](te, codec)
    new PartitionParquet[F, A, K](rdd, ate, cfg.withFormat(Parquet), bucketing, pathBuilder)
  }

  // 12
  def avro: PartitionAvro[F, A, K] =
    new PartitionAvro[F, A, K](rdd, codec, cfg.withFormat(Avro), bucketing, pathBuilder)

// 13
  def binAvro: PartitionBinaryAvro[F, A, K] =
    new PartitionBinaryAvro[F, A, K](rdd, codec, cfg.withFormat(BinaryAvro), bucketing, pathBuilder)

// 14
  def objectFile: PartitionObjectFile[F, A, K] =
    new PartitionObjectFile[F, A, K](rdd, cfg.withFormat(JavaObject), bucketing, pathBuilder)

// 15
  def protobuf(implicit ev: A <:< GeneratedMessage): PartitionProtobuf[F, A, K] =
    new PartitionProtobuf[F, A, K](rdd, codec, cfg.withFormat(ProtoBuf), bucketing, pathBuilder)
}
