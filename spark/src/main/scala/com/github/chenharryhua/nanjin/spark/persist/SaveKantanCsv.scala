package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.pipes.CsvSerde
import com.github.chenharryhua.nanjin.terminals.{withOptionalHeader, NEWLINE_SEPERATOR}
import kantan.csv.CsvConfiguration.Header
import kantan.csv.{CsvConfiguration, HeaderEncoder}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.sql.Dataset

final class SaveKantanCsv[F[_], A](
  ds: Dataset[A],
  val csvConfiguration: CsvConfiguration,
  cfg: HoarderConfig,
  encoder: HeaderEncoder[A])
    extends Serializable {

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): SaveKantanCsv[F, A] =
    new SaveKantanCsv[F, A](ds, f(csvConfiguration), cfg, encoder)

  // header
  def withHeader: SaveKantanCsv[F, A]              = updateCsvConfig(_.withHeader)
  def withHeader(ss: String*): SaveKantanCsv[F, A] = updateCsvConfig(_.withHeader(CsvConfiguration.Header.Explicit(ss)))
  def withoutHeader: SaveKantanCsv[F, A]           = updateCsvConfig(_.withoutHeader)

  // quote
  def quoteAll: SaveKantanCsv[F, A]              = updateCsvConfig(_.quoteAll)
  def quoteWhenNeeded: SaveKantanCsv[F, A]       = updateCsvConfig(_.quoteWhenNeeded)
  def withQuote(char: Char): SaveKantanCsv[F, A] = updateCsvConfig(_.withQuote(char))

  // seperator
  def withCellSeparator(char: Char): SaveKantanCsv[F, A] = updateCsvConfig(_.withCellSeparator(char))

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveKantanCsv[F, A] =
    new SaveKantanCsv[F, A](ds, csvConfiguration, cfg, encoder)

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
        saveRDD.csv[A](
          ds.rdd,
          params.outPath,
          params.compression,
          csvConfiguration,
          withOptionalHeader(encoder, ds.schema.fields.map(_.name).toIndexedSeq))
      })
}

private class KantanCsvIterator[A](headerEncoder: HeaderEncoder[A], csvCfg: CsvConfiguration, iter: Iterator[A])
    extends Iterator[(NullWritable, Text)] {

  private[this] val nullWritable: NullWritable = NullWritable.get()

  private val headerText: Option[(NullWritable, Text)] = {
    val headerStrs: Option[Seq[String]] = csvCfg.header match {
      case Header.None             => None
      case Header.Implicit         => headerEncoder.header
      case Header.Explicit(header) => Some(header)
    }
    headerStrs.map(hs => (nullWritable, new Text(hs.mkString(csvCfg.cellSeparator.toString) + NEWLINE_SEPERATOR)))
  }

  private[this] var isFirstTimeAccess: Boolean = true

  private[this] def nextText(): Text = new Text(CsvSerde.rowEncode(iter.next(), csvCfg)(headerEncoder.rowEncoder))

  override def hasNext: Boolean = iter.hasNext

  override def next(): (NullWritable, Text) =
    if (isFirstTimeAccess) {
      isFirstTimeAccess = false
      headerText match {
        case Some(value) => value
        case None        => (nullWritable, nextText())
      }
    } else (nullWritable, nextText())
}
