package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class RddFileSaver[F[_], A: ClassTag](rdd: RDD[A], cfg: SaverConfig = SaverConfig())(implicit
  codec: NJAvroCodec[A],
  ss: SparkSession)
    extends Serializable {

  private def updateConfig(cfg: SaverConfig): RddFileSaver[F, A] =
    new RddFileSaver[F, A](rdd, cfg)

  def overwrite: RddFileSaver[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: RddFileSaver[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: RddFileSaver[F, A] = updateConfig(cfg.withIgnore)

  def single: RddFileSaver[F, A] = updateConfig(cfg.withSingle)
  def multi: RddFileSaver[F, A]  = updateConfig(cfg.withMulti)

  def spark: RddFileSaver[F, A] = updateConfig(cfg.withSpark)
  def raw: RddFileSaver[F, A]   = updateConfig(cfg.withRaw)

  val params: SaverParams = cfg.evalConfig

  private def sma = new SaveModeAware[F](params.saveMode, ss)

  def avro(outPath: String): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, outPath, sma, cfg)

  def parquet(outPath: String): SaveParquet[F, A] =
    new SaveParquet[F, A](rdd, outPath, sma, cfg)

  def jackson(outPath: String): SaveJackson[F, A] =
    new SaveJackson[F, A](rdd, outPath, sma, cfg)

  def binAvro(outPath: String): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, outPath, sma)

  def circe(outPath: String)(implicit ev: JsonEncoder[A]): SaveCirce[F, A] =
    new SaveCirce[F, A](rdd, outPath, sma, cfg)

  def json(outPath: String) =
    new SaveJson[F, A](rdd, outPath, sma)

  def csv(outPath: String)(implicit ev: RowEncoder[A]): SaveCSV[F, A] =
    new SaveCSV[F, A](rdd, outPath, CsvConfiguration.rfc, sma, cfg)

  def text(outPath: String)(implicit ev: Show[A]): SaveText[F, A] =
    new SaveText[F, A](rdd, outPath, sma, cfg)

  def objectFile(outPath: String) = new SaveObjectFile[F, A](rdd, outPath)
}

final class SaveObjectFile[F[_], A: ClassTag](rdd: RDD[A], outPath: String) extends Serializable {

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    F.delay(rdd.saveAsObjectFile(outPath))
}
