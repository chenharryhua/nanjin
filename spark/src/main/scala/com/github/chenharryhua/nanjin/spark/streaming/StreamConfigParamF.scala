package com.github.chenharryhua.nanjin.spark.streaming

import cats.Functor
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.spark.NJShowDataset
import higherkindness.droste.data.Fix
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

@Lenses final case class StreamParams private (
  timeRange: NJDateTimeRange,
  showDs: NJShowDataset,
  fileFormat: NJFileFormat,
  checkpoint: NJCheckpoint,
  dataLoss: NJFailOnDataLoss,
  outputMode: OutputMode,
  trigger: Trigger)

object StreamParams {

  def apply(tr: NJDateTimeRange, sd: NJShowDataset, ff: NJFileFormat): StreamParams =
    StreamParams(
      tr,
      sd,
      ff,
      NJCheckpoint("./data/checkpoint/"),
      NJFailOnDataLoss(true),
      OutputMode.Append,
      Trigger.ProcessingTime(0)
    )
}

sealed private[spark] trait StreamConfigParamF[A]

private[spark] object StreamConfigParamF {

  final case class DefaultParams[K](tr: NJDateTimeRange, showDs: NJShowDataset, ff: NJFileFormat)
      extends StreamConfigParamF[K]

  final case class WithCheckpointReplace[K](value: String, cont: K) extends StreamConfigParamF[K]
  final case class WithCheckpointAppend[K](value: String, cont: K) extends StreamConfigParamF[K]

  final case class WithFailOnDataLoss[K](value: Boolean, cont: K) extends StreamConfigParamF[K]
  final case class WithOutputMode[K](value: OutputMode, cont: K) extends StreamConfigParamF[K]
  final case class WithTrigger[K](value: Trigger, cont: K) extends StreamConfigParamF[K]

  implicit val configParamFunctor: Functor[StreamConfigParamF] =
    cats.derived.semi.functor[StreamConfigParamF]

  type ConfigParam = Fix[StreamConfigParamF]

  private val algebra: Algebra[StreamConfigParamF, StreamParams] =
    Algebra[StreamConfigParamF, StreamParams] {
      case DefaultParams(tr, sd, ff)   => StreamParams(tr, sd, ff)
      case WithCheckpointReplace(v, c) => StreamParams.checkpoint.set(NJCheckpoint(v))(c)
      case WithCheckpointAppend(v, c)  => StreamParams.checkpoint.modify(_.append(v))(c)
      case WithFailOnDataLoss(v, c)    => StreamParams.dataLoss.set(NJFailOnDataLoss(v))(c)
      case WithOutputMode(v, c)        => StreamParams.outputMode.set(v)(c)
      case WithTrigger(v, c)           => StreamParams.trigger.set(v)(c)
    }

  def evalParams(params: ConfigParam): StreamParams = scheme.cata(algebra).apply(params)

  def apply(tr: NJDateTimeRange, sd: NJShowDataset, ff: NJFileFormat): ConfigParam =
    Fix(DefaultParams[ConfigParam](tr, sd, ff))

  def withCheckpoint(cp: String, cont: ConfigParam): ConfigParam =
    Fix(WithCheckpointReplace(cp, cont))

  def withCheckpointAppend(cp: String, cont: ConfigParam): ConfigParam =
    Fix(WithCheckpointAppend(cp, cont))

  def withFailOnDataLoss(dl: Boolean, cont: ConfigParam): ConfigParam =
    Fix(WithFailOnDataLoss(dl, cont))

  def withOutputMode(f: OutputMode, cont: ConfigParam): ConfigParam =
    Fix(WithOutputMode(f, cont))

  def withTrigger(trigger: Trigger, cont: ConfigParam): ConfigParam =
    Fix(WithTrigger(trigger, cont))

}
