package com.github.chenharryhua.nanjin.guard.config

import cats.{Functor, Show}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.generic.JsonCodec
import monocle.macros.Lenses

@JsonCodec @Lenses
final case class ActionParams(
  metricId: MetricID,
  isCritical: Boolean,
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
    retryPolicy: Policy,
    serviceParams: ServiceParams
  ): ActionParams =
    ActionParams(
      metricId =
        MetricID(serviceParams, measurement, Category.Timer(TimerKind.ActionTimer), actionName.value),
      isCritical = false,
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
  final case class WithCritical[K](value: Boolean, cont: K) extends ActionConfigF[K]

  def algebra(
    actionName: ActionName,
    measurement: Measurement,
    retryPolicy: Policy): Algebra[ActionConfigF, ActionParams] =
    Algebra[ActionConfigF, ActionParams] {
      case InitParams(serviceParams) => ActionParams(actionName, measurement, retryPolicy, serviceParams)
      case WithPublishStrategy(v, c) => ActionParams.publishStrategy.set(v)(c)
      case WithTiming(v, c)          => ActionParams.isTiming.set(v)(c)
      case WithCounting(v, c)        => ActionParams.isCounting.set(v)(c)
      case WithCritical(v, c)        => ActionParams.isCritical.set(v)(c)
    }
}

final case class ActionConfig private (private val cont: Fix[ActionConfigF]) {
  import ActionConfigF.*

  def silent: ActionConfig =
    ActionConfig(Fix(WithPublishStrategy(PublishStrategy.Silent, cont)))
  def aware: ActionConfig =
    ActionConfig(Fix(WithPublishStrategy(PublishStrategy.CompleteOnly, cont)))
  def notice: ActionConfig =
    ActionConfig(Fix(WithPublishStrategy(PublishStrategy.StartAndComplete, cont)))

  def critical: ActionConfig   = ActionConfig(Fix(WithCritical(value = true, cont)))
  def uncritical: ActionConfig = ActionConfig(Fix(WithCritical(value = false, cont)))

  def withCounting: ActionConfig    = ActionConfig(Fix(WithCounting(value = true, cont)))
  def withTiming: ActionConfig      = ActionConfig(Fix(WithTiming(value = true, cont)))
  def withoutCounting: ActionConfig = ActionConfig(Fix(WithCounting(value = false, cont)))
  def withoutTiming: ActionConfig   = ActionConfig(Fix(WithTiming(value = false, cont)))

  def evalConfig(actionName: ActionName, measurement: Measurement, retryPolicy: Policy): ActionParams =
    scheme.cata(algebra(actionName, measurement, retryPolicy)).apply(cont)
}

private[guard] object ActionConfig {

  def apply(serviceParams: ServiceParams): ActionConfig =
    ActionConfig(Fix(ActionConfigF.InitParams[Fix[ActionConfigF]](serviceParams)))
}
