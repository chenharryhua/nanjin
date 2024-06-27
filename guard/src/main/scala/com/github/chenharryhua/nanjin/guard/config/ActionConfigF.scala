package com.github.chenharryhua.nanjin.guard.config

import cats.Functor
import cats.data.Reader
import com.github.chenharryhua.nanjin.common.chrono.{policies, Policy}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.generic.JsonCodec
import monocle.syntax.all.*

@JsonCodec
final case class ActionParams(
  actionName: ActionName,
  measurement: Measurement,
  importance: Importance,
  publishStrategy: PublishStrategy,
  isCounting: Boolean,
  isTiming: Boolean,
  retryPolicy: Policy,
  serviceParams: ServiceParams) {
  val configStr: String = {
    val cc = if (isCounting) ".counted" else ""
    val tc = if (isTiming) ".timed" else ""
    s"${publishStrategy.entryName}.${importance.entryName}$tc$cc"
  }

  val metricName: MetricName = MetricName(serviceParams, measurement, actionName.value)
}

object ActionParams {
  def apply(
    actionName: ActionName,
    measurement: Measurement,
    serviceParams: ServiceParams
  ): ActionParams =
    ActionParams(
      actionName = actionName,
      measurement = measurement,
      importance = Importance.Normal,
      publishStrategy = PublishStrategy.Silent,
      isCounting = false,
      isTiming = false,
      retryPolicy = policies.giveUp,
      serviceParams = serviceParams
    )
}

sealed private[guard] trait ActionConfigF[X]

private object ActionConfigF {
  implicit val functorActionConfigF: Functor[ActionConfigF] = cats.derived.semiauto.functor[ActionConfigF]

  final case class InitParams[K](
    actionName: ActionName,
    measurement: Measurement,
    serviceParams: ServiceParams)
      extends ActionConfigF[K]

  final case class WithPublishStrategy[K](value: PublishStrategy, cont: K) extends ActionConfigF[K]
  final case class WithTiming[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithCounting[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithImportance[K](value: Importance, cont: K) extends ActionConfigF[K]
  final case class WithRetryPolicy[K](value: Policy, cont: K) extends ActionConfigF[K]
  final case class WithMeasurement[K](value: Measurement, cont: K) extends ActionConfigF[K]

  val algebra: Algebra[ActionConfigF, ActionParams] =
    Algebra[ActionConfigF, ActionParams] {
      case InitParams(actionName, measurement, serviceParams) =>
        ActionParams(actionName, measurement, serviceParams)
      case WithPublishStrategy(v, c) => c.focus(_.publishStrategy).replace(v)
      case WithTiming(v, c)          => c.focus(_.isTiming).replace(v)
      case WithCounting(v, c)        => c.focus(_.isCounting).replace(v)
      case WithImportance(v, c)      => c.focus(_.importance).replace(v)
      case WithRetryPolicy(v, c)     => c.focus(_.retryPolicy).replace(v)
      case WithMeasurement(v, c)     => c.focus(_.measurement).replace(v)
    }
}

final class ActionConfig private (
  private[guard] val isWorthRetry: Reader[Throwable, Boolean],
  cont: Fix[ActionConfigF]) {
  import ActionConfigF.*

  private def strategy(ps: PublishStrategy): ActionConfig =
    new ActionConfig(isWorthRetry, Fix(WithPublishStrategy(ps, cont)))
  def bipartite: ActionConfig  = strategy(PublishStrategy.Bipartite)
  def unipartite: ActionConfig = strategy(PublishStrategy.Unipartite)
  def silent: ActionConfig     = strategy(PublishStrategy.Silent)

  private def importance(im: Importance): ActionConfig =
    new ActionConfig(isWorthRetry, Fix(WithImportance(value = im, cont)))
  def critical: ActionConfig      = importance(Importance.Critical)
  def normal: ActionConfig        = importance(Importance.Normal)
  def insignificant: ActionConfig = importance(Importance.Insignificant)
  def suppressed: ActionConfig    = importance(Importance.Suppressed)

  def counted: ActionConfig =
    new ActionConfig(isWorthRetry, Fix(WithCounting(value = true, cont)))

  def timed: ActionConfig =
    new ActionConfig(isWorthRetry, Fix(WithTiming(value = true, cont)))

  def policy(retryPolicy: Policy): ActionConfig =
    new ActionConfig(isWorthRetry, Fix(WithRetryPolicy(retryPolicy, cont)))

  def withMeasurement(measurement: String): ActionConfig =
    new ActionConfig(isWorthRetry, Fix(WithMeasurement(Measurement(measurement), cont)))

  def worthRetry(f: Throwable => Boolean) =
    new ActionConfig(Reader(f), cont)

  private[guard] def evalConfig: ActionParams = scheme.cata(algebra).apply(cont)
}

object ActionConfig {

  def apply(actionName: ActionName, measurement: Measurement, serviceParams: ServiceParams): ActionConfig =
    new ActionConfig(
      Reader(_ => true),
      Fix(ActionConfigF.InitParams[Fix[ActionConfigF]](actionName, measurement, serviceParams)))
}
