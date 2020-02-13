package com.github.chenharryhua.nanjin.spark.kafka

import java.time.{LocalDateTime, ZoneId}

import cats.Functor
import cats.data.Reader
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.common.TopicName
import com.github.chenharryhua.nanjin.spark.{NJRepartition, NJShowDataset}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy}

import scala.concurrent.duration._

@Lenses final case class NJUploadRate(batchSize: Int, duration: FiniteDuration)

object NJUploadRate {
  val default: NJUploadRate = NJUploadRate(batchSize = 1000, duration = 1.second)
}

final case class NJPathBuild(fileFormat: NJFileFormat, topicName: TopicName)

@Lenses final case class SKParams private (
  timeRange: NJDateTimeRange,
  uploadRate: NJUploadRate,
  pathBuilder: Reader[NJPathBuild, String],
  fileFormat: NJFileFormat,
  saveMode: SaveMode,
  locationStrategy: LocationStrategy,
  repartition: NJRepartition,
  showDs: NJShowDataset)

object SKParams {

  val default: SKParams =
    SKParams(
      timeRange        = NJDateTimeRange.infinite,
      uploadRate       = NJUploadRate.default,
      pathBuilder      = Reader(kpb => s"./data/spark/kafka/${kpb.topicName}/${kpb.fileFormat}/"),
      fileFormat       = NJFileFormat.Parquet,
      saveMode         = SaveMode.ErrorIfExists,
      locationStrategy = LocationStrategies.PreferConsistent,
      repartition      = NJRepartition(30),
      showDs           = NJShowDataset(60, isTruncate = false)
    )
}

sealed trait ConfigParamF[A]

object ConfigParamF {
  final case class DefaultParams[K]() extends ConfigParamF[K]

  final case class WithBatchSize[K](value: Int, cont: K) extends ConfigParamF[K]
  final case class WithDuration[K](value: FiniteDuration, cont: K) extends ConfigParamF[K]

  final case class WithFileFormat[K](value: NJFileFormat, cont: K) extends ConfigParamF[K]

  final case class WithStartTime[K](value: LocalDateTime, cont: K) extends ConfigParamF[K]
  final case class WithEndTime[K](value: LocalDateTime, cont: K) extends ConfigParamF[K]
  final case class WithZoneId[K](value: ZoneId, cont: K) extends ConfigParamF[K]

  final case class WithSaveMode[K](value: SaveMode, cont: K) extends ConfigParamF[K]
  final case class WithLocationStrategy[K](value: LocationStrategy, cont: K) extends ConfigParamF[K]

  final case class WithRepartition[K](value: NJRepartition, cont: K) extends ConfigParamF[K]
  final case class WithShowRows[K](value: Int, cont: K) extends ConfigParamF[K]
  final case class WithShowTruncate[K](value: Boolean, cont: K) extends ConfigParamF[K]

  final case class WithPathBuilder[K](value: Reader[NJPathBuild, String], cont: K)
      extends ConfigParamF[K]

  implicit val configParamFunctor: Functor[ConfigParamF] = cats.derived.semi.functor[ConfigParamF]

  type ConfigParam = Fix[ConfigParamF]

  private val algebra: Algebra[ConfigParamF, SKParams] = Algebra[ConfigParamF, SKParams] {
    case DefaultParams()            => SKParams.default
    case WithBatchSize(v, c)        => SKParams.uploadRate.composeLens(NJUploadRate.batchSize).set(v)(c)
    case WithDuration(v, c)         => SKParams.uploadRate.composeLens(NJUploadRate.duration).set(v)(c)
    case WithFileFormat(v, c)       => SKParams.fileFormat.set(v)(c)
    case WithStartTime(v, c)        => SKParams.timeRange.modify(_.withStartTime(v))(c)
    case WithEndTime(v, c)          => SKParams.timeRange.modify(_.withEndTime(v))(c)
    case WithZoneId(v, c)           => SKParams.timeRange.modify(_.withZoneId(v))(c)
    case WithSaveMode(v, c)         => SKParams.saveMode.set(v)(c)
    case WithLocationStrategy(v, c) => SKParams.locationStrategy.set(v)(c)
    case WithRepartition(v, c)      => SKParams.repartition.set(v)(c)
    case WithShowRows(v, c)         => SKParams.showDs.composeLens(NJShowDataset.rowNum).set(v)(c)
    case WithShowTruncate(v, c)     => SKParams.showDs.composeLens(NJShowDataset.isTruncate).set(v)(c)
    case WithPathBuilder(v, c)      => SKParams.pathBuilder.set(v)(c)
  }

  def evalParams(params: ConfigParam): SKParams = scheme.cata(algebra).apply(params)

  val defaultParams: ConfigParam = Fix(DefaultParams[ConfigParam]())

  def withBatchSize(v: Int, cont: ConfigParam): ConfigParam           = Fix(WithBatchSize(v, cont))
  def withDuration(v: FiniteDuration, cont: ConfigParam): ConfigParam = Fix(WithDuration(v, cont))

  def withFileFormat(ff: NJFileFormat, cont: ConfigParam): Fix[ConfigParamF] =
    Fix(WithFileFormat(ff, cont))

  def withStartTime(s: LocalDateTime, cont: ConfigParam): ConfigParam = Fix(WithStartTime(s, cont))
  def withEndTime(s: LocalDateTime, cont: ConfigParam): ConfigParam   = Fix(WithEndTime(s, cont))
  def withZoneId(s: ZoneId, cont: ConfigParam): ConfigParam           = Fix(WithZoneId(s, cont))

  def withLocationStrategy(ls: LocationStrategy, cont: ConfigParam): ConfigParam =
    Fix(WithLocationStrategy(ls, cont))

  def withRepartition(rp: Int, cont: ConfigParam): ConfigParam =
    Fix(WithRepartition(NJRepartition(rp), cont))

  def withShowRows(num: Int, cont: ConfigParam): ConfigParam       = Fix(WithShowRows(num, cont))
  def withShowTruncate(t: Boolean, cont: ConfigParam): ConfigParam = Fix(WithShowTruncate(t, cont))

  def withSaveMode(sm: SaveMode, cont: ConfigParam): ConfigParam = Fix(WithSaveMode(sm, cont))

  def withPathBuilder(f: Reader[NJPathBuild, String], cont: ConfigParam): ConfigParam =
    Fix(WithPathBuilder(f, cont))
}
