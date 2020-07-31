package com.github.chenharryhua.nanjin.spark.streaming

import java.util.concurrent.TimeUnit

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.spark.NJShowDataset
import higherkindness.droste.data.Fix
import higherkindness.droste.macros.deriveTraverse
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

final private[spark] case class NJCheckpoint(value: String) extends AnyVal {

  def append(sub: String): NJCheckpoint = {
    val s = if (sub.startsWith("/")) sub.tail else sub
    val v = if (value.endsWith("/")) value.dropRight(1) else value
    NJCheckpoint(s"$v/$s")
  }
}

final private[spark] case class NJFailOnDataLoss(value: Boolean) extends AnyVal

@Lenses final private[spark] case class NJStreamParams private (
  timeRange: NJDateTimeRange,
  showDs: NJShowDataset,
  fileFormat: NJFileFormat,
  checkpoint: NJCheckpoint,
  dataLoss: NJFailOnDataLoss,
  outputMode: OutputMode,
  trigger: Trigger)

private[spark] object NJStreamParams {

  def apply(tr: NJDateTimeRange, sd: NJShowDataset): NJStreamParams =
    NJStreamParams(
      timeRange = tr,
      showDs = sd,
      fileFormat = NJFileFormat.Parquet,
      checkpoint = NJCheckpoint("./data/checkpoint/"),
      dataLoss = NJFailOnDataLoss(true),
      outputMode = OutputMode.Append,
      trigger = Trigger.ProcessingTime(0)
    )
}

@deriveTraverse sealed private[spark] trait NJStreamConfigF[A]

private[spark] object NJStreamConfigF {

  final case class DefaultParams[K](tr: NJDateTimeRange, showDs: NJShowDataset)
      extends NJStreamConfigF[K]

  final case class WithCheckpointReplace[K](value: String, cont: K) extends NJStreamConfigF[K]
  final case class WithCheckpointAppend[K](value: String, cont: K) extends NJStreamConfigF[K]

  final case class WithFailOnDataLoss[K](isFail: Boolean, cont: K) extends NJStreamConfigF[K]
  final case class WithOutputMode[K](value: OutputMode, cont: K) extends NJStreamConfigF[K]
  final case class WithTrigger[K](value: Trigger, cont: K) extends NJStreamConfigF[K]

  private val algebra: Algebra[NJStreamConfigF, NJStreamParams] =
    Algebra[NJStreamConfigF, NJStreamParams] {
      case DefaultParams(tr, sd)       => NJStreamParams(tr, sd)
      case WithCheckpointReplace(v, c) => NJStreamParams.checkpoint.set(NJCheckpoint(v))(c)
      case WithCheckpointAppend(v, c)  => NJStreamParams.checkpoint.modify(_.append(v))(c)
      case WithFailOnDataLoss(v, c)    => NJStreamParams.dataLoss.set(NJFailOnDataLoss(v))(c)
      case WithOutputMode(v, c)        => NJStreamParams.outputMode.set(v)(c)
      case WithTrigger(v, c)           => NJStreamParams.trigger.set(v)(c)
    }

  def evalConfig(cfg: NJStreamConfig): NJStreamParams = scheme.cata(algebra).apply(cfg.value)

}

final private[spark] case class NJStreamConfig(value: Fix[NJStreamConfigF]) extends AnyVal {
  import NJStreamConfigF._

  def withCheckpointReplace(cp: String): NJStreamConfig =
    NJStreamConfig(Fix(WithCheckpointReplace(cp, value)))

  def withCheckpointAppend(cp: String): NJStreamConfig =
    NJStreamConfig(Fix(WithCheckpointAppend(cp, value)))

  def failOnDataLoss: NJStreamConfig =
    NJStreamConfig(Fix(WithFailOnDataLoss(isFail = true, value)))

  def ignoreDataLoss: NJStreamConfig =
    NJStreamConfig(Fix(WithFailOnDataLoss(isFail = false, value)))

  private def withOutputMode(f: OutputMode): NJStreamConfig =
    NJStreamConfig(Fix(WithOutputMode(f, value)))
  def withAppend: NJStreamConfig   = withOutputMode(OutputMode.Append())
  def withComplete: NJStreamConfig = withOutputMode(OutputMode.Complete())
  def withUpdate: NJStreamConfig   = withOutputMode(OutputMode.Update())

  def withTrigger(trigger: Trigger): NJStreamConfig =
    NJStreamConfig(Fix(WithTrigger(trigger, value)))

  def withProcessingTimeTrigger(ms: Long): NJStreamConfig =
    withTrigger(Trigger.ProcessingTime(ms, TimeUnit.MILLISECONDS))

  def withContinousTrigger(ms: Long): NJStreamConfig =
    withTrigger(Trigger.Continuous(ms, TimeUnit.MILLISECONDS))

  def evalConfig: NJStreamParams = NJStreamConfigF.evalConfig(this)
}

private[spark] object NJStreamConfig {

  def apply(tr: NJDateTimeRange, sd: NJShowDataset): NJStreamConfig =
    NJStreamConfig(Fix(NJStreamConfigF.DefaultParams[Fix[NJStreamConfigF]](tr, sd)))
}
