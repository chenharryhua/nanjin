package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.{fileSink, utils, RddExt}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class SaveCSV[F[_], A: ClassTag](
  rdd: RDD[A],
  csvConfiguration: CsvConfiguration,
  cfg: HoarderConfig)(implicit rowEncoder: RowEncoder[A], codec: NJAvroCodec[A], ss: SparkSession)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration) =
    new SaveCSV[F, A](rdd, f(csvConfiguration), cfg)

  private def updateConfig(cfg: HoarderConfig): SaveCSV[F, A] =
    new SaveCSV[F, A](rdd, csvConfiguration, cfg)

  def single: SaveCSV[F, A] = updateConfig(cfg.withSingle)
  def multi: SaveCSV[F, A]  = updateConfig(cfg.withMulti)

  def overwrite: SaveCSV[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveCSV[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveCSV[F, A] = updateConfig(cfg.withIgnore)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)
    params.singleOrMulti match {
      case SingleOrMulti.Single =>
        sma.checkAndRun(blocker)(
          rdd
            .map(codec.idConversion)
            .stream[F]
            .through(fileSink[F](blocker).csv(params.outPath, csvConfiguration))
            .compile
            .drain)
      case SingleOrMulti.Multi =>
        val csv = F.delay(
          utils
            .normalizedDF(rdd, codec.avroEncoder)
            .write
            .mode(SaveMode.Overwrite)
            .option("sep", csvConfiguration.cellSeparator.toString)
            .option("header", csvConfiguration.hasHeader)
            .option("quote", csvConfiguration.quote.toString)
            .option("charset", "UTF8")
            .csv(params.outPath))
        sma.checkAndRun(blocker)(csv)
    }
  }
}
