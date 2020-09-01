package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class RddFileHoader[F[_], A: ClassTag](
  rdd: RDD[A],
  cfg: HoarderConfig = HoarderConfig.default)(implicit codec: NJAvroCodec[A], ss: SparkSession)
    extends Serializable {

  private def updateConfig(cfg: HoarderConfig): RddFileHoader[F, A] =
    new RddFileHoader[F, A](rdd, cfg)

  def overwrite: RddFileHoader[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: RddFileHoader[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: RddFileHoader[F, A] = updateConfig(cfg.withIgnore)

  def avro(outPath: String): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, cfg.withFormat(NJFileFormat.Avro).withOutPutPath(outPath))

  def parquet(outPath: String): SaveParquet[F, A] =
    new SaveParquet[F, A](rdd, cfg.withFormat(NJFileFormat.Parquet).withOutPutPath(outPath))

  def jackson(outPath: String): SaveJackson[F, A] =
    new SaveJackson[F, A](rdd, cfg.withFormat(NJFileFormat.Jackson).withOutPutPath(outPath))

  def binAvro(outPath: String): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, cfg.withFormat(NJFileFormat.BinaryAvro).withOutPutPath(outPath))

  def circe(outPath: String)(implicit ev: JsonEncoder[A]): SaveCirce[F, A] =
    new SaveCirce[F, A](rdd, cfg.withFormat(NJFileFormat.Circe).withOutPutPath(outPath))

  def json(outPath: String) =
    new SaveSparkJson[F, A](rdd, cfg.withFormat(NJFileFormat.SparkJson).withOutPutPath(outPath))

  def csv(outPath: String)(implicit ev: RowEncoder[A]): SaveCSV[F, A] =
    new SaveCSV[F, A](
      rdd,
      CsvConfiguration.rfc,
      cfg.withFormat(NJFileFormat.Csv).withOutPutPath(outPath))

  def text(outPath: String)(implicit ev: Show[A]): SaveText[F, A] =
    new SaveText[F, A](rdd, cfg.withFormat(NJFileFormat.Text).withOutPutPath(outPath))

  def objectFile(outPath: String): SaveObjectFile[F, A] =
    new SaveObjectFile[F, A](rdd, cfg.withFormat(NJFileFormat.JavaObject).withOutPutPath(outPath))
}
