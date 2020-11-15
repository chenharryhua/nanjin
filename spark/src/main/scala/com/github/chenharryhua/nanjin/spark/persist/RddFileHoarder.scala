package com.github.chenharryhua.nanjin.spark.persist

import com.github.chenharryhua.nanjin.common.NJFileFormat._
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import frameless.TypedEncoder
import kantan.csv.CsvConfiguration
import org.apache.spark.rdd.RDD

final class RddFileHoarder[F[_], A](
  rdd: RDD[A],
  codec: AvroCodec[A],
  cfg: HoarderConfig = HoarderConfig.default)
    extends Serializable {

  private def updateConfig(cfg: HoarderConfig): RddFileHoarder[F, A] =
    new RddFileHoarder[F, A](rdd, codec, cfg)

  def overwrite: RddFileHoarder[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: RddFileHoarder[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: RddFileHoarder[F, A] = updateConfig(cfg.withIgnore)

  def repartition(num: Int): RddFileHoarder[F, A] =
    new RddFileHoarder[F, A](rdd.repartition(num), codec, cfg)

// 1
  def jackson(outPath: String): SaveJackson[F, A] =
    new SaveJackson[F, A](rdd, codec, cfg.withFormat(Jackson).withOutPutPath(outPath))

// 2
  def circe(outPath: String): SaveCirce[F, A] =
    new SaveCirce[F, A](
      rdd,
      codec,
      cfg.withFormat(Circe).withOutPutPath(outPath),
      isKeepNull = true)

// 3
  def text(outPath: String): SaveText[F, A] =
    new SaveText[F, A](rdd, codec, cfg.withFormat(Text).withOutPutPath(outPath), ".txt")

// 4
  def csv(outPath: String)(implicit te: TypedEncoder[A]): SaveCsv[F, A] = {
    val ate: AvroTypedEncoder[A] = AvroTypedEncoder[A](te, codec)
    new SaveCsv[F, A](rdd, ate, CsvConfiguration.rfc, cfg.withFormat(Csv).withOutPutPath(outPath))
  }

  // 5
  def json(outPath: String)(implicit te: TypedEncoder[A]): SaveSparkJson[F, A] = {
    val ate: AvroTypedEncoder[A] = AvroTypedEncoder[A](te, codec)
    new SaveSparkJson[F, A](
      rdd,
      ate,
      cfg.withFormat(SparkJson).withOutPutPath(outPath),
      isKeepNull = true)
  }

  // 11
  def parquet(outPath: String)(implicit te: TypedEncoder[A]): SaveParquet[F, A] = {
    val ate: AvroTypedEncoder[A] = AvroTypedEncoder[A](te, codec)
    new SaveParquet[F, A](rdd, ate, cfg.withFormat(Parquet).withOutPutPath(outPath))
  }

  // 12
  def avro(outPath: String): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, codec, cfg.withFormat(Avro).withOutPutPath(outPath))

// 13
  def binAvro(outPath: String): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, codec, cfg.withFormat(BinaryAvro).withOutPutPath(outPath))

// 14
  def objectFile(outPath: String): SaveObjectFile[F, A] =
    new SaveObjectFile[F, A](rdd, cfg.withFormat(JavaObject).withOutPutPath(outPath))

// 15
  def protobuf(outPath: String): SaveProtobuf[F, A] =
    new SaveProtobuf[F, A](rdd, codec, cfg.withFormat(ProtoBuf).withOutPutPath(outPath))
}
