package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes.CsvSerialization
import com.github.chenharryhua.nanjin.spark.RddExt
import kantan.csv.CsvConfiguration.QuotePolicy
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

final class SaveCsv[F[_], A](ds: Dataset[A], csvConfiguration: CsvConfiguration, cfg: HoarderConfig)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): SaveCsv[F, A] =
    new SaveCsv[F, A](ds, f(csvConfiguration), cfg)

  def withHeader: SaveCsv[F, A]                    = updateCsvConfig(_.withHeader)
  def withoutHeader: SaveCsv[F, A]                 = updateCsvConfig(_.withoutHeader)
  def quoteAll: SaveCsv[F, A]                      = updateCsvConfig(_.quoteAll)
  def quoteWhenNeeded: SaveCsv[F, A]               = updateCsvConfig(_.quoteWhenNeeded)
  def withQuote(char: Char): SaveCsv[F, A]         = updateCsvConfig(_.withQuote(char))
  def withCellSeparator(char: Char): SaveCsv[F, A] = updateCsvConfig(_.withCellSeparator(char))

  private def updateConfig(cfg: HoarderConfig): SaveCsv[F, A] =
    new SaveCsv[F, A](ds, csvConfiguration, cfg)

  def overwrite: SaveCsv[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveCsv[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveCsv[F, A] = updateConfig(cfg.withIgnore)

  def outPath(path: String): SaveCsv[F, A] = updateConfig(cfg.withOutPutPath(path))

  def file: SaveCsv[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveCsv[F, A] = updateConfig(cfg.withFolder)

  def gzip: SaveCsv[F, A] = updateConfig(cfg.withCompression(Compression.Gzip))

  def deflate(level: Int): SaveCsv[F, A] =
    updateConfig(cfg.withCompression(Compression.Deflate(level)))

  def run(blocker: Blocker)(implicit
    F: Concurrent[F],
    cs: ContextShift[F],
    ss: SparkSession,
    rowEncoder: RowEncoder[A]): F[Unit] = {
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)
    val ccg: CompressionCodecGroup[F] =
      params.compression.ccg[F](ss.sparkContext.hadoopConfiguration)
    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        val hadoop = NJHadoop[F](ss.sparkContext.hadoopConfiguration, blocker)

        val csvConf =
          if (csvConfiguration.hasHeader)
            csvConfiguration.withHeader(ds.schema.fieldNames: _*)
          else csvConfiguration

        val pipe = new CsvSerialization[F, A](csvConf)

        sma.checkAndRun(blocker)(
          ds.rdd
            .stream[F]
            .through(pipe.serialize(blocker))
            .through(ccg.compressionPipe)
            .through(hadoop.byteSink(params.outPath))
            .compile
            .drain)

      case FolderOrFile.Folder =>
        val quoteAll = csvConfiguration.quotePolicy match {
          case QuotePolicy.Always     => true
          case QuotePolicy.WhenNeeded => false
        }
        val csv = F.delay(
          ds.write
            .mode(SaveMode.Overwrite)
            .option("compression", ccg.name)
            .option("sep", csvConfiguration.cellSeparator.toString)
            .option("header", csvConfiguration.hasHeader)
            .option("quote", csvConfiguration.quote.toString)
            .option("quoteAll", quoteAll)
            .option("charset", "UTF8")
            .csv(params.outPath))
        sma.checkAndRun(blocker)(csv)
    }
  }
}
