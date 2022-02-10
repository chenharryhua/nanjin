package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import kantan.csv.CsvConfiguration
import kantan.csv.CsvConfiguration.QuotePolicy
import org.apache.spark.sql.Dataset

final class SaveKantanCsv[F[_], A](ds: Dataset[A], val csvConfiguration: CsvConfiguration, cfg: HoarderConfig)
    extends Serializable {

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): SaveKantanCsv[F, A] =
    new SaveKantanCsv[F, A](ds, f(csvConfiguration), cfg)

  def withHeader: SaveKantanCsv[F, A]                    = updateCsvConfig(_.withHeader)
  def withoutHeader: SaveKantanCsv[F, A]                 = updateCsvConfig(_.withoutHeader)
  def quoteAll: SaveKantanCsv[F, A]                      = updateCsvConfig(_.quoteAll)
  def quoteWhenNeeded: SaveKantanCsv[F, A]               = updateCsvConfig(_.quoteWhenNeeded)
  def withQuote(char: Char): SaveKantanCsv[F, A]         = updateCsvConfig(_.withQuote(char))
  def withCellSeparator(char: Char): SaveKantanCsv[F, A] = updateCsvConfig(_.withCellSeparator(char))

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveKantanCsv[F, A] =
    new SaveKantanCsv[F, A](ds, csvConfiguration, cfg)

  def append: SaveKantanCsv[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveKantanCsv[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveKantanCsv[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveKantanCsv[F, A] = updateConfig(cfg.ignoreMode)

  def bzip2: SaveKantanCsv[F, A]               = updateConfig(cfg.outputCompression(NJCompression.Bzip2))
  def gzip: SaveKantanCsv[F, A]                = updateConfig(cfg.outputCompression(NJCompression.Gzip))
  def deflate(level: Int): SaveKantanCsv[F, A] = updateConfig(cfg.outputCompression(NJCompression.Deflate(level)))
  def uncompress: SaveKantanCsv[F, A]          = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, ds.sparkSession.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptibleMany {
        val quoteAll: Boolean = csvConfiguration.quotePolicy match {
          case QuotePolicy.Always     => true
          case QuotePolicy.WhenNeeded => false
        }
        ds.write
          .mode(params.saveMode)
          .option("compression", params.compression.name)
          .option("sep", csvConfiguration.cellSeparator.toString)
          .option("header", csvConfiguration.hasHeader)
          .option("quote", csvConfiguration.quote.toString)
          .option("quoteAll", quoteAll)
          .option("charset", "UTF8")
          .csv(params.outPath.pathStr)
      })
}
