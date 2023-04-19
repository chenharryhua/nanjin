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
    actionName: String,
    measurement: Measurement,
    retryPolicy: String,
    serviceParams: ServiceParams
  ): ActionParams =
    ActionParams(
      metricId = MetricID(serviceParams, measurement, Category.Timer(TimerKind.ActionTimer), actionName),
      isCritical = false,
      publishStrategy = PublishStrategy.Silent,
      isCounting = false,
      isTiming = false,
      retryPolicy = retryPolicy,
      serviceParams = serviceParams
    )
}

sealed private[guard] trait ActionConfigF[X]

private object ActionConfigF {
  implicit val functorActionConfigF: Functor[ActionConfigF] = cats.derived.semiauto.functor[ActionConfigF]

  final case class InitParams[K]() extends ActionConfigF[K]

  final case class WithPublishStrategy[K](value: PublishStrategy, cont: K) extends ActionConfigF[K]
  final case class WithTiming[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithCounting[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithCritical[K](value: Boolean, cont: K) extends ActionConfigF[K]

  def algebra(
    actionName: String,
    measurement: Measurement,
    serviceParams: ServiceParams,
    retryPolicy: String): Algebra[ActionConfigF, ActionParams] =
    Algebra[ActionConfigF, ActionParams] {
      case InitParams()              => ActionParams(actionName, measurement, retryPolicy, serviceParams)
      case WithPublishStrategy(v, c) => ActionParams.publishStrategy.set(v)(c)
      case WithTiming(v, c)          => ActionParams.isTiming.set(v)(c)
      case WithCounting(v, c)        => ActionParams.isCounting.set(v)(c)
      case WithCritical(v, c)        => ActionParams.isCritical.set(v)(c)
    }
}

final case class ActionConfig private (private val cont: Fix[ActionConfigF], serviceParams: ServiceParams) {
  import ActionConfigF.*

  def silent: ActionConfig =
    ActionConfig(Fix(WithPublishStrategy(PublishStrategy.Silent, cont)), serviceParams)
  def aware: ActionConfig =
    ActionConfig(Fix(WithPublishStrategy(PublishStrategy.CompleteOnly, cont)), serviceParams)
  def notice: ActionConfig =
    ActionConfig(Fix(WithPublishStrategy(PublishStrategy.StartAndComplete, cont)), serviceParams)

  def critical: ActionConfig   = ActionConfig(Fix(WithCritical(value = true, cont)), serviceParams)
  def uncritical: ActionConfig = ActionConfig(Fix(WithCritical(value = false, cont)), serviceParams)

  def withCounting: ActionConfig    = ActionConfig(Fix(WithCounting(value = true, cont)), serviceParams)
  def withTiming: ActionConfig      = ActionConfig(Fix(WithTiming(value = true, cont)), serviceParams)
  def withoutCounting: ActionConfig = ActionConfig(Fix(WithCounting(value = false, cont)), serviceParams)
  def withoutTiming: ActionConfig   = ActionConfig(Fix(WithTiming(value = false, cont)), serviceParams)

  def evalConfig(actionName: String, measurement: Measurement, retryPolicy: String): ActionParams =
    scheme.cata(algebra(actionName, measurement, serviceParams, retryPolicy)).apply(cont)
}

private[guard] object ActionConfig {

  def apply(serviceParams: ServiceParams): ActionConfig =
    ActionConfig(Fix(ActionConfigF.InitParams[Fix[ActionConfigF]]()), serviceParams)
}
