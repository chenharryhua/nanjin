package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.pipes.KantanSerde
import com.github.chenharryhua.nanjin.terminals.{KantanCompression, NJCompression}
import kantan.csv.{CsvConfiguration, HeaderEncoder, RowEncoder}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.sql.Dataset

final class SaveKantanCsv[F[_], A](
  val dataset: Dataset[A],
  val csvConfiguration: CsvConfiguration,
  cfg: HoarderConfig,
  encoder: HeaderEncoder[A])
    extends Serializable {

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): SaveKantanCsv[F, A] =
    new SaveKantanCsv[F, A](dataset, f(csvConfiguration), cfg, encoder)

  // header
  def withHeader: SaveKantanCsv[F, A] = updateCsvConfig(_.withHeader)
  def withHeader(ss: String*): SaveKantanCsv[F, A] = updateCsvConfig(
    _.withHeader(CsvConfiguration.Header.Explicit(ss)))
  def withoutHeader: SaveKantanCsv[F, A] = updateCsvConfig(_.withoutHeader)

  // quote
  def quoteAll: SaveKantanCsv[F, A]              = updateCsvConfig(_.quoteAll)
  def quoteWhenNeeded: SaveKantanCsv[F, A]       = updateCsvConfig(_.quoteWhenNeeded)
  def withQuote(char: Char): SaveKantanCsv[F, A] = updateCsvConfig(_.withQuote(char))

  // seperator
  def withCellSeparator(char: Char): SaveKantanCsv[F, A] = updateCsvConfig(_.withCellSeparator(char))

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveKantanCsv[F, A] =
    new SaveKantanCsv[F, A](dataset, csvConfiguration, cfg, encoder)

  def append: SaveKantanCsv[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveKantanCsv[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveKantanCsv[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveKantanCsv[F, A] = updateConfig(cfg.ignoreMode)

  def bzip2: SaveKantanCsv[F, A] = updateConfig(cfg.outputCompression(NJCompression.Bzip2))
  def deflate(level: Int): SaveKantanCsv[F, A] = updateConfig(
    cfg.outputCompression(NJCompression.Deflate(level)))
  def gzip: SaveKantanCsv[F, A]       = updateConfig(cfg.outputCompression(NJCompression.Gzip))
  def lz4: SaveKantanCsv[F, A]        = updateConfig(cfg.outputCompression(NJCompression.Lz4))
  def uncompress: SaveKantanCsv[F, A] = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))
  def snappy: SaveKantanCsv[F, A]     = updateConfig(cfg.outputCompression(NJCompression.Snappy))

  def withCompression(kc: KantanCompression): SaveKantanCsv[F, A] = updateConfig(cfg.outputCompression(kc))

  private def withOptionalHeader(encoder: HeaderEncoder[A]): HeaderEncoder[A] =
    new HeaderEncoder[A] {
      override val header: Option[Seq[String]] =
        encoder.header.orElse(Some(dataset.schema.fields.map(_.name).toIndexedSeq))
      override val rowEncoder: RowEncoder[A] = encoder.rowEncoder
    }

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](
      params.saveMode,
      params.outPath,
      dataset.sparkSession.sparkContext.hadoopConfiguration).checkAndRun(F.interruptibleMany {
      saveRDD.csv[A](
        dataset.rdd,
        params.outPath,
        params.compression,
        csvConfiguration,
        withOptionalHeader(encoder))
    })
}

private class KantanCsvIterator[A](
  headerEncoder: HeaderEncoder[A],
  csvCfg: CsvConfiguration,
  iter: Iterator[A])
    extends Iterator[(NullWritable, Text)] {

  private[this] val nullWritable: NullWritable = NullWritable.get()

  private[this] var isFirstTimeAccess: Boolean = true

  private[this] def nextText(): Text = new Text(
    KantanSerde.rowEncode(iter.next(), csvCfg, headerEncoder.rowEncoder))

  override def hasNext: Boolean = iter.hasNext

  override def next(): (NullWritable, Text) =
    if (isFirstTimeAccess) {
      isFirstTimeAccess = false
      (nullWritable, new Text(KantanSerde.headerStr(csvCfg, headerEncoder)))
    } else (nullWritable, nextText())
}
