package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.pipes.serde.{CsvSerde, NEWLINE_SEPERATOR}
import kantan.csv.CsvConfiguration.Header
import kantan.csv.{CsvConfiguration, HeaderEncoder}
import monocle.macros.GenLens
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.sql.Dataset

final class SaveKantanCsv[F[_], A](
  ds: Dataset[A],
  csvCfg: CsvConfiguration,
  cfg: HoarderConfig,
  encoder: HeaderEncoder[A])
    extends Serializable {

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): SaveKantanCsv[F, A] =
    new SaveKantanCsv[F, A](ds, f(csvCfg), cfg, encoder)

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

  lazy val csvConfiguration: CsvConfiguration =
    GenLens[CsvConfiguration](_.header).modify {
      case Header.Implicit => Header.Explicit(ds.schema.fields.map(_.name).toSeq)
      case others          => others
    }(csvCfg)

  private def updateConfig(cfg: HoarderConfig): SaveKantanCsv[F, A] =
    new SaveKantanCsv[F, A](ds, csvCfg, cfg, encoder)

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
        saveRDD.kantanCsv[A](ds.rdd, params.outPath, params.compression, csvConfiguration, encoder)
      })
}

private class KantanCsvIterator[A](enc: HeaderEncoder[A], conf: CsvConfiguration, iter: Iterator[A])
    extends Iterator[(NullWritable, Text)] {

  private[this] val nullWritable: NullWritable = NullWritable.get()

  private val headerText: Option[(NullWritable, Text)] = {
    val headerStrs: Option[Seq[String]] = conf.header match {
      case Header.None             => None
      case Header.Implicit         => enc.header
      case Header.Explicit(header) => Some(header)
    }
    headerStrs.map(hs => (nullWritable, new Text(hs.mkString(conf.cellSeparator.toString) + NEWLINE_SEPERATOR)))
  }

  private[this] var isFirstTimeAccess: Boolean = true

  private[this] def nextText(): Text = new Text(CsvSerde.rowEncode(iter.next(), conf)(enc.rowEncoder))

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
