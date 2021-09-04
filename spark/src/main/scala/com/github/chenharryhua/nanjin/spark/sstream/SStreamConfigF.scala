package com.github.chenharryhua.nanjin.spark.sstream

import cats.Functor
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.time.ZoneId
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

final private[spark] case class NJFailOnDataLoss(value: Boolean) extends AnyVal

@Lenses final private[sstream] case class SStreamParams private (
  zoneId: ZoneId,
  fileFormat: NJFileFormat,
  checkpointBuilder: NJFileFormat => String,
  dataLoss: NJFailOnDataLoss,
  outputMode: OutputMode,
  trigger: Trigger,
  progressInterval: FiniteDuration,
  queryName: Option[String]) {
  val checkpoint: String = checkpointBuilder(fileFormat)
}

private[sstream] object SStreamParams {

  def apply(zoneId: ZoneId): SStreamParams =
    SStreamParams(
      zoneId = zoneId,
      fileFormat = NJFileFormat.SparkJson,
      checkpointBuilder = (fmt: NJFileFormat) => s"./data/checkpoint/sstream/${fmt.format}",
      dataLoss = NJFailOnDataLoss(true),
      outputMode = OutputMode.Append,
      trigger = Trigger.ProcessingTime(1, TimeUnit.MINUTES),
      progressInterval = FiniteDuration(5, TimeUnit.SECONDS),
      queryName = None
    )
}

sealed private[sstream] trait SStreamConfigF[A]

private object SStreamConfigF {
  implicit val functorSStreamConfigF: Functor[SStreamConfigF] = cats.derived.semiauto.functor[SStreamConfigF]

  final case class InitParams[K](zoneId: ZoneId) extends SStreamConfigF[K]

  final case class WithCheckpointBuilder[K](f: NJFileFormat => String, cont: K) extends SStreamConfigF[K]

  final case class WithFailOnDataLoss[K](isFail: Boolean, cont: K) extends SStreamConfigF[K]
  final case class WithOutputMode[K](value: OutputMode, cont: K) extends SStreamConfigF[K]
  final case class WithTrigger[K](value: Trigger, cont: K) extends SStreamConfigF[K]

  final case class WithFormat[K](value: NJFileFormat, cont: K) extends SStreamConfigF[K]
  final case class WithProgressInterval[K](value: FiniteDuration, cont: K) extends SStreamConfigF[K]

  final case class WithQueryName[K](value: String, cont: K) extends SStreamConfigF[K]

  private val algebra: Algebra[SStreamConfigF, SStreamParams] =
    Algebra[SStreamConfigF, SStreamParams] {
      case InitParams(tr)              => SStreamParams(tr)
      case WithCheckpointBuilder(v, c) => SStreamParams.checkpointBuilder.set(v)(c)
      case WithFailOnDataLoss(v, c)    => SStreamParams.dataLoss.set(NJFailOnDataLoss(v))(c)
      case WithOutputMode(v, c)        => SStreamParams.outputMode.set(v)(c)
      case WithTrigger(v, c)           => SStreamParams.trigger.set(v)(c)
      case WithFormat(v, c)            => SStreamParams.fileFormat.set(v)(c)
      case WithProgressInterval(v, c)  => SStreamParams.progressInterval.set(v)(c)
      case WithQueryName(v, c)         => SStreamParams.queryName.set(Some(v))(c)
    }

  def evalConfig(cfg: SStreamConfig): SStreamParams = scheme.cata(algebra).apply(cfg.value)

}

final private[sstream] case class SStreamConfig(value: Fix[SStreamConfigF]) extends AnyVal {
  import SStreamConfigF.*

  def checkpointBuilder(f: NJFileFormat => String): SStreamConfig =
    SStreamConfig(Fix(WithCheckpointBuilder(f, value)))
  def checkpoint(cp: String): SStreamConfig = checkpointBuilder(_ => cp)

  def dataLossFailure: SStreamConfig = SStreamConfig(Fix(WithFailOnDataLoss(isFail = true, value)))
  def dataLossIgnore: SStreamConfig  = SStreamConfig(Fix(WithFailOnDataLoss(isFail = false, value)))

  private def withOutputMode(f: OutputMode): SStreamConfig = SStreamConfig(Fix(WithOutputMode(f, value)))
  def appendMode: SStreamConfig                            = withOutputMode(OutputMode.Append())
  def completeMode: SStreamConfig                          = withOutputMode(OutputMode.Complete())
  def updateMode: SStreamConfig                            = withOutputMode(OutputMode.Update())

  def triggerMode(trigger: Trigger): SStreamConfig = SStreamConfig(Fix(WithTrigger(trigger, value)))

  def jsonFormat: SStreamConfig    = SStreamConfig(Fix(WithFormat(NJFileFormat.SparkJson, value)))
  def parquetFormat: SStreamConfig = SStreamConfig(Fix(WithFormat(NJFileFormat.Parquet, value)))
  def avroFormat: SStreamConfig    = SStreamConfig(Fix(WithFormat(NJFileFormat.Avro, value)))

  def progressInterval(fd: FiniteDuration): SStreamConfig = SStreamConfig(Fix(WithProgressInterval(fd, value)))
  def progressInterval(ms: Long): SStreamConfig           = progressInterval(FiniteDuration(ms, TimeUnit.MILLISECONDS))

  def queryName(name: String): SStreamConfig = SStreamConfig(Fix(WithQueryName(name, value)))

  def evalConfig: SStreamParams = SStreamConfigF.evalConfig(this)
}

private[spark] object SStreamConfig {

  def apply(zoneId: ZoneId): SStreamConfig =
    SStreamConfig(Fix(SStreamConfigF.InitParams[Fix[SStreamConfigF]](zoneId)))
}
