package com.github.chenharryhua.nanjin.guard.config

import cats.{Functor, Show}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.generic.JsonCodec
import monocle.syntax.all.*

@JsonCodec
final case class ActionParams(
  metricId: MetricID,
  importance: Importance,
  publishStrategy: PublishStrategy,
  isCounting: Boolean,
  isTiming: Boolean,
  retryPolicy: String, // for display
  serviceParams: ServiceParams)

object ActionParams {
  implicit val showActionParams: Show[ActionParams] = cats.derived.semiauto.show

  def apply(
    actionName: ActionName,
    measurement: Measurement,
    retryPolicy: ServicePolicy,
    serviceParams: ServiceParams
  ): ActionParams =
    ActionParams(
      metricId =
        MetricID(serviceParams, measurement, Category.Timer(TimerKind.ActionTimer), actionName.value),
      importance = Importance.Normal,
      publishStrategy = PublishStrategy.Silent,
      isCounting = false,
      isTiming = false,
      retryPolicy = retryPolicy.value,
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

  def algebra(
    actionName: ActionName,
    measurement: Measurement,
    retryPolicy: ServicePolicy): Algebra[ActionConfigF, ActionParams] =
    Algebra[ActionConfigF, ActionParams] {
      case InitParams(serviceParams) => ActionParams(actionName, measurement, retryPolicy, serviceParams)
      case WithPublishStrategy(v, c) => c.focus(_.publishStrategy).replace(v)
      case WithTiming(v, c)          => c.focus(_.isTiming).replace(v)
      case WithCounting(v, c)        => c.focus(_.isCounting).replace(v)
      case WithImportance(v, c)      => c.focus(_.importance).replace(v)
    }
}

final case class ActionConfig(cont: Fix[ActionConfigF]) extends AnyVal {
  import ActionConfigF.*

  def notice: ActionConfig = ActionConfig(Fix(WithPublishStrategy(PublishStrategy.Notice, cont)))
  def aware: ActionConfig  = ActionConfig(Fix(WithPublishStrategy(PublishStrategy.Aware, cont)))
  def silent: ActionConfig = ActionConfig(Fix(WithPublishStrategy(PublishStrategy.Silent, cont)))

  def critical: ActionConfig      = ActionConfig(Fix(WithImportance(value = Importance.Critical, cont)))
  def normal: ActionConfig        = ActionConfig(Fix(WithImportance(value = Importance.Normal, cont)))
  def insignificant: ActionConfig = ActionConfig(Fix(WithImportance(value = Importance.Insignificant, cont)))
  def trivial: ActionConfig       = ActionConfig(Fix(WithImportance(value = Importance.Trivial, cont)))

  def withCounting: ActionConfig    = ActionConfig(Fix(WithCounting(value = true, cont)))
  def withTiming: ActionConfig      = ActionConfig(Fix(WithTiming(value = true, cont)))
  def withoutCounting: ActionConfig = ActionConfig(Fix(WithCounting(value = false, cont)))
  def withoutTiming: ActionConfig   = ActionConfig(Fix(WithTiming(value = false, cont)))

  def evalConfig(actionName: ActionName, measurement: Measurement, retryPolicy: ServicePolicy): ActionParams =
    scheme.cata(algebra(actionName, measurement, retryPolicy)).apply(cont)
}

object ActionConfig {

  def apply(serviceParams: ServiceParams): ActionConfig =
    ActionConfig(Fix(ActionConfigF.InitParams[Fix[ActionConfigF]](serviceParams)))
}
