package com.github.chenharryhua.nanjin.spark.persist

import cats.data.Kleisli
import cats.effect.kernel.{Async, Sync}
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.spark.RddExt
import fs2.{Pipe, Stream}
import kantan.csv.CsvConfiguration.QuotePolicy
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Dataset
import squants.information.Information

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

  def file: SaveSingleCsv[F, A]  = new SaveSingleCsv[F, A](ds, csvConfiguration, cfg, None)
  def folder: SaveMultiCsv[F, A] = new SaveMultiCsv[F, A](ds, csvConfiguration, cfg)
}

final class SaveSingleCsv[F[_], A](
  ds: Dataset[A],
  csvConfiguration: CsvConfiguration,
  cfg: HoarderConfig,
  listener: Option[Kleisli[F, A, Unit]])
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSingleCsv[F, A] =
    new SaveSingleCsv[F, A](ds, csvConfiguration, cfg, listener)

  def overwrite: SaveSingleCsv[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveSingleCsv[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveSingleCsv[F, A] = updateConfig(cfg.ignoreMode)

  def gzip: SaveSingleCsv[F, A]                = updateConfig(cfg.outputCompression(Compression.Gzip))
  def deflate(level: Int): SaveSingleCsv[F, A] = updateConfig(cfg.outputCompression(Compression.Deflate(level)))
  def uncompress: SaveSingleCsv[F, A]          = updateConfig(cfg.outputCompression(Compression.Uncompressed))

  def withChunkSize(cs: ChunkSize): SaveSingleCsv[F, A]    = updateConfig(cfg.chunkSize(cs))
  def withByteBuffer(bb: Information): SaveSingleCsv[F, A] = updateConfig(cfg.byteBuffer(bb))
  def withListener(f: A => F[Unit]): SaveSingleCsv[F, A] =
    new SaveSingleCsv[F, A](ds, csvConfiguration, cfg, Some(Kleisli(f)))

  def sink(implicit F: Async[F], rowEncoder: RowEncoder[A]): Stream[F, Unit] = {
    val hc: Configuration     = ds.sparkSession.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)
    val csvConf: CsvConfiguration =
      if (csvConfiguration.hasHeader)
        csvConfiguration.withHeader(ds.schema.fieldNames.toIndexedSeq*)
      else csvConfiguration

    val src: Stream[F, A] = ds.rdd.stream[F](params.chunkSize)
    val tgt: Pipe[F, A, Unit] =
      sinks.csv(params.outPath, hc, csvConf, params.compression.fs2Compression, params.byteBuffer)
    val ss: Stream[F, Unit] = listener.fold(src.through(tgt))(k => src.evalTap(k.run).through(tgt))

    sma.checkAndRun(ss)
  }
}

final class SaveMultiCsv[F[_], A](ds: Dataset[A], csvConfiguration: CsvConfiguration, cfg: HoarderConfig)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiCsv[F, A] =
    new SaveMultiCsv[F, A](ds, csvConfiguration, cfg)

  def append: SaveMultiCsv[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveMultiCsv[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveMultiCsv[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveMultiCsv[F, A] = updateConfig(cfg.ignoreMode)

  def bzip2: SaveMultiCsv[F, A]               = updateConfig(cfg.outputCompression(Compression.Bzip2))
  def gzip: SaveMultiCsv[F, A]                = updateConfig(cfg.outputCompression(Compression.Gzip))
  def deflate(level: Int): SaveMultiCsv[F, A] = updateConfig(cfg.outputCompression(Compression.Deflate(level)))
  def uncompress: SaveMultiCsv[F, A]          = updateConfig(cfg.outputCompression(Compression.Uncompressed))

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
