package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class RddFileHoader[F[_], A: ClassTag](rdd: RDD[A], cfg: HoarderConfig = HoarderConfig())(
  implicit
  codec: NJAvroCodec[A],
  ss: SparkSession)
    extends Serializable {

  private def updateConfig(cfg: HoarderConfig): RddFileHoader[F, A] =
    new RddFileHoader[F, A](rdd, cfg)

  def overwrite: RddFileHoader[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: RddFileHoader[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: RddFileHoader[F, A] = updateConfig(cfg.withIgnore)

  def avro(outPath: String): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, outPath, cfg)

  def parquet(outPath: String): SaveParquet[F, A] =
    new SaveParquet[F, A](rdd, outPath, cfg)

  def jackson(outPath: String): SaveJackson[F, A] =
    new SaveJackson[F, A](rdd, outPath, cfg)

  def binAvro(outPath: String): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, outPath, cfg)

  def circe(outPath: String)(implicit ev: JsonEncoder[A]): SaveCirce[F, A] =
    new SaveCirce[F, A](rdd, outPath, cfg)

  def json(outPath: String) =
    new SaveJson[F, A](rdd, outPath, cfg)

  def csv(outPath: String)(implicit ev: RowEncoder[A]): SaveCSV[F, A] =
    new SaveCSV[F, A](rdd, outPath, CsvConfiguration.rfc, cfg)

  def text(outPath: String)(implicit ev: Show[A]): SaveText[F, A] =
    new SaveText[F, A](rdd, outPath, cfg)

  def objectFile(outPath: String): SaveObjectFile[F, A] =
    new SaveObjectFile[F, A](rdd, outPath)
}
