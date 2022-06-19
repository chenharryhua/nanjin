package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.{KantanCompression, NJCompression}
import kantan.csv.{CsvConfiguration, HeaderEncoder}
import org.apache.spark.rdd.RDD
import shapeless.{HList, LabelledGeneric}
import shapeless.ops.record.Keys
import shapeless.ops.hlist.ToTraversable
import scala.annotation.nowarn
final class SaveKantanCsv[F[_], A](
  rdd: RDD[A],
  val csvConfiguration: CsvConfiguration,
  cfg: HoarderConfig,
  encoder: HeaderEncoder[A])
    extends Serializable {

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): SaveKantanCsv[F, A] =
    new SaveKantanCsv[F, A](rdd, f(csvConfiguration), cfg, encoder)

  // https://svejcar.dev/posts/2019/10/22/extracting-case-class-field-names-with-shapeless/
  def withHeader[Repr <: HList, KeysRepr <: HList](implicit
    @nowarn gen: LabelledGeneric.Aux[A, Repr],
    keys: Keys.Aux[Repr, KeysRepr],
    traversable: ToTraversable.Aux[KeysRepr, List, Symbol]): SaveKantanCsv[F, A] = {
    def fieldNames: List[String] = keys().toList.map(_.name)
    updateCsvConfig(_.withHeader(fieldNames*))
  }

  def withHeader(ss: String*): SaveKantanCsv[F, A] =
    updateCsvConfig(_.withHeader(CsvConfiguration.Header.Explicit(ss)))
  def withoutHeader: SaveKantanCsv[F, A] = updateCsvConfig(_.withoutHeader)

  // quote
  def quoteAll: SaveKantanCsv[F, A]              = updateCsvConfig(_.quoteAll)
  def quoteWhenNeeded: SaveKantanCsv[F, A]       = updateCsvConfig(_.quoteWhenNeeded)
  def withQuote(char: Char): SaveKantanCsv[F, A] = updateCsvConfig(_.withQuote(char))

  // seperator
  def withCellSeparator(char: Char): SaveKantanCsv[F, A] = updateCsvConfig(_.withCellSeparator(char))

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveKantanCsv[F, A] =
    new SaveKantanCsv[F, A](rdd, csvConfiguration, cfg, encoder)

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
  // def snappy: SaveKantanCsv[F, A]     = updateConfig(cfg.outputCompression(NJCompression.Snappy))

  def withCompression(kc: KantanCompression): SaveKantanCsv[F, A] = updateConfig(cfg.outputCompression(kc))

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptibleMany {
        saveRDD.kantan[A](rdd, params.outPath, params.compression, csvConfiguration, encoder)
      })
}
