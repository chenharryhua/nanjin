package com.github.chenharryhua.nanjin.guard.config

import cats.Functor
import com.github.chenharryhua.nanjin.common.chrono.{policies, Policy}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.generic.JsonCodec
import monocle.syntax.all.*

@JsonCodec
final case class ActionParams(
  metricName: MetricName,
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
}

object ActionParams {

  def apply(
    actionName: ActionName,
    measurement: Measurement,
    serviceParams: ServiceParams
  ): ActionParams =
    ActionParams(
      metricName = MetricName(serviceParams, measurement, actionName.value),
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

  final case class InitParams[K](value: ServiceParams) extends ActionConfigF[K]

  final case class WithPublishStrategy[K](value: PublishStrategy, cont: K) extends ActionConfigF[K]
  final case class WithTiming[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithCounting[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithImportance[K](value: Importance, cont: K) extends ActionConfigF[K]
  final case class WithRetryPolicy[K](value: Policy, cont: K) extends ActionConfigF[K]

  def algebra(actionName: ActionName, measurement: Measurement): Algebra[ActionConfigF, ActionParams] =
    Algebra[ActionConfigF, ActionParams] {
      case InitParams(serviceParams) => ActionParams(actionName, measurement, serviceParams)
      case WithPublishStrategy(v, c) => c.focus(_.publishStrategy).replace(v)
      case WithTiming(v, c)          => c.focus(_.isTiming).replace(v)
      case WithCounting(v, c)        => c.focus(_.isCounting).replace(v)
      case WithImportance(v, c)      => c.focus(_.importance).replace(v)
      case WithRetryPolicy(v, c)     => c.focus(_.retryPolicy).replace(v)
    }
}

final case class ActionConfig(cont: Fix[ActionConfigF]) {
  import ActionConfigF.*

  def bipartite: ActionConfig  = ActionConfig(Fix(WithPublishStrategy(PublishStrategy.Bipartite, cont)))
  def unipartite: ActionConfig = ActionConfig(Fix(WithPublishStrategy(PublishStrategy.Unipartite, cont)))
  def silent: ActionConfig     = ActionConfig(Fix(WithPublishStrategy(PublishStrategy.Silent, cont)))

  def critical: ActionConfig      = ActionConfig(Fix(WithImportance(value = Importance.Critical, cont)))
  def normal: ActionConfig        = ActionConfig(Fix(WithImportance(value = Importance.Normal, cont)))
  def insignificant: ActionConfig = ActionConfig(Fix(WithImportance(value = Importance.Insignificant, cont)))
  def suppressed: ActionConfig    = ActionConfig(Fix(WithImportance(value = Importance.Suppressed, cont)))

  def counted: ActionConfig   = ActionConfig(Fix(WithCounting(value = true, cont)))
  def timed: ActionConfig     = ActionConfig(Fix(WithTiming(value = true, cont)))
  def uncounted: ActionConfig = ActionConfig(Fix(WithCounting(value = false, cont)))
  def untimed: ActionConfig   = ActionConfig(Fix(WithTiming(value = false, cont)))

  def policy(retryPolicy: Policy): ActionConfig = ActionConfig(Fix(WithRetryPolicy(retryPolicy, cont)))

  def evalConfig(actionName: ActionName, measurement: Measurement): ActionParams =
    scheme.cata(algebra(actionName, measurement)).apply(cont)
}

object ActionConfig {

  def apply(serviceParams: ServiceParams): ActionConfig =
    ActionConfig(Fix(ActionConfigF.InitParams[Fix[ActionConfigF]](serviceParams)))
}
