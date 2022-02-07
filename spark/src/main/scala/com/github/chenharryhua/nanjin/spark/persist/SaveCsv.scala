package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import kantan.csv.CsvConfiguration
import kantan.csv.CsvConfiguration.QuotePolicy
import org.apache.spark.sql.Dataset

final class SaveCsv[F[_], A](ds: Dataset[A], csvConfiguration: CsvConfiguration, cfg: HoarderConfig)
    extends Serializable {

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): SaveCsv[F, A] =
    new SaveCsv[F, A](ds, f(csvConfiguration), cfg)

  def withHeader: SaveCsv[F, A]                    = updateCsvConfig(_.withHeader)
  def withoutHeader: SaveCsv[F, A]                 = updateCsvConfig(_.withoutHeader)
  def quoteAll: SaveCsv[F, A]                      = updateCsvConfig(_.quoteAll)
  def quoteWhenNeeded: SaveCsv[F, A]               = updateCsvConfig(_.quoteWhenNeeded)
  def withQuote(char: Char): SaveCsv[F, A]         = updateCsvConfig(_.withQuote(char))
  def withCellSeparator(char: Char): SaveCsv[F, A] = updateCsvConfig(_.withCellSeparator(char))

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveCsv[F, A] =
    new SaveCsv[F, A](ds, csvConfiguration, cfg)

  def append: SaveCsv[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveCsv[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveCsv[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveCsv[F, A] = updateConfig(cfg.ignoreMode)

  def bzip2: SaveCsv[F, A]               = updateConfig(cfg.outputCompression(NJCompression.Bzip2))
  def gzip: SaveCsv[F, A]                = updateConfig(cfg.outputCompression(NJCompression.Gzip))
  def deflate(level: Int): SaveCsv[F, A] = updateConfig(cfg.outputCompression(NJCompression.Deflate(level)))
  def uncompress: SaveCsv[F, A]          = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))

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
