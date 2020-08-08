package com.github.chenharryhua.nanjin.spark.sstream

import java.util.concurrent.TimeUnit

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.spark.{NJCheckpoint, NJFailOnDataLoss, NJShowDataset}
import higherkindness.droste.data.Fix
import higherkindness.droste.macros.deriveTraverse
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

@Lenses final private[spark] case class NJSStreamParams private (
  timeRange: NJDateTimeRange,
  showDs: NJShowDataset,
  fileFormat: NJFileFormat,
  checkpoint: NJCheckpoint,
  dataLoss: NJFailOnDataLoss,
  outputMode: OutputMode,
  trigger: Trigger)

private[spark] object NJSStreamParams {

  def apply(tr: NJDateTimeRange, sd: NJShowDataset): NJSStreamParams =
    NJSStreamParams(
      timeRange = tr,
      showDs = sd,
      fileFormat = NJFileFormat.Jackson,
      checkpoint = NJCheckpoint("./data/sstream/checkpoint/"),
      dataLoss = NJFailOnDataLoss(true),
      outputMode = OutputMode.Append,
      trigger = Trigger.ProcessingTime(1, TimeUnit.MINUTES)
    )
}

@deriveTraverse sealed private[spark] trait NJSStreamConfigF[A]

private[spark] object NJSStreamConfigF {

  final case class DefaultParams[K](tr: NJDateTimeRange, showDs: NJShowDataset)
      extends NJSStreamConfigF[K]

  final case class WithCheckpointReplace[K](value: String, cont: K) extends NJSStreamConfigF[K]
  final case class WithCheckpointAppend[K](value: String, cont: K) extends NJSStreamConfigF[K]

  final case class WithFailOnDataLoss[K](isFail: Boolean, cont: K) extends NJSStreamConfigF[K]
  final case class WithOutputMode[K](value: OutputMode, cont: K) extends NJSStreamConfigF[K]
  final case class WithTrigger[K](value: Trigger, cont: K) extends NJSStreamConfigF[K]

  final case class WithFormat[K](value: NJFileFormat, cont: K) extends NJSStreamConfigF[K]

  private val algebra: Algebra[NJSStreamConfigF, NJSStreamParams] =
    Algebra[NJSStreamConfigF, NJSStreamParams] {
      case DefaultParams(tr, sd)       => NJSStreamParams(tr, sd)
      case WithCheckpointReplace(v, c) => NJSStreamParams.checkpoint.set(NJCheckpoint(v))(c)
      case WithCheckpointAppend(v, c)  => NJSStreamParams.checkpoint.modify(_.append(v))(c)
      case WithFailOnDataLoss(v, c)    => NJSStreamParams.dataLoss.set(NJFailOnDataLoss(v))(c)
      case WithOutputMode(v, c)        => NJSStreamParams.outputMode.set(v)(c)
      case WithTrigger(v, c)           => NJSStreamParams.trigger.set(v)(c)
      case WithFormat(v, c)            => NJSStreamParams.fileFormat.set(v)(c)
    }

  def evalConfig(cfg: NJSStreamConfig): NJSStreamParams = scheme.cata(algebra).apply(cfg.value)

}

final private[spark] case class NJSStreamConfig(value: Fix[NJSStreamConfigF]) extends AnyVal {
  import NJSStreamConfigF._

  def withCheckpointReplace(cp: String): NJSStreamConfig =
    NJSStreamConfig(Fix(WithCheckpointReplace(cp, value)))

  def withCheckpointAppend(cp: String): NJSStreamConfig =
    NJSStreamConfig(Fix(WithCheckpointAppend(cp, value)))

  def failOnDataLoss: NJSStreamConfig =
    NJSStreamConfig(Fix(WithFailOnDataLoss(isFail = true, value)))

  def ignoreDataLoss: NJSStreamConfig =
    NJSStreamConfig(Fix(WithFailOnDataLoss(isFail = false, value)))

  private def withOutputMode(f: OutputMode): NJSStreamConfig =
    NJSStreamConfig(Fix(WithOutputMode(f, value)))
  def withAppend: NJSStreamConfig   = withOutputMode(OutputMode.Append())
  def withComplete: NJSStreamConfig = withOutputMode(OutputMode.Complete())
  def withUpdate: NJSStreamConfig   = withOutputMode(OutputMode.Update())

  def withTrigger(trigger: Trigger): NJSStreamConfig =
    NJSStreamConfig(Fix(WithTrigger(trigger, value)))

  def withProcessingTimeTrigger(ms: Long): NJSStreamConfig =
    withTrigger(Trigger.ProcessingTime(ms, TimeUnit.MILLISECONDS))

  def withContinousTrigger(ms: Long): NJSStreamConfig =
    withTrigger(Trigger.Continuous(ms, TimeUnit.MILLISECONDS))

  def withJson: NJSStreamConfig    = NJSStreamConfig(Fix(WithFormat(NJFileFormat.Jackson, value)))
  def withParquet: NJSStreamConfig = NJSStreamConfig(Fix(WithFormat(NJFileFormat.Parquet, value)))
  def withAvro: NJSStreamConfig    = NJSStreamConfig(Fix(WithFormat(NJFileFormat.Avro, value)))

  def evalConfig: NJSStreamParams = NJSStreamConfigF.evalConfig(this)
}

private[spark] object NJSStreamConfig {

  def apply(tr: NJDateTimeRange, sd: NJShowDataset): NJSStreamConfig =
    NJSStreamConfig(Fix(NJSStreamConfigF.DefaultParams[Fix[NJSStreamConfigF]](tr, sd)))
}
