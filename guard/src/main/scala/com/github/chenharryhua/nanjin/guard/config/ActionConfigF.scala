package com.github.chenharryhua.nanjin.guard.config

import cats.{Functor, Show}
import cats.implicits.catsSyntaxPartialOrder
import higherkindness.droste.{scheme, Algebra}
import higherkindness.droste.data.Fix
import io.circe.generic.JsonCodec
import monocle.macros.Lenses

@JsonCodec @Lenses
final case class ActionParams private (
  name: String,
  importance: Importance,
  isCounting: Boolean,
  isTiming: Boolean,
  retryPolicy: String, // for display
  serviceParams: ServiceParams) {
  val digested: Digested = Digested(serviceParams, name)

  val isCritical: Boolean   = importance > Importance.High // Critical
  val isNotice: Boolean     = importance > Importance.Medium // Hight + Critical
  val isNonTrivial: Boolean = importance > Importance.Low // Medium + High + Critical

}

object ActionParams {
  implicit val showActionParams: Show[ActionParams] = cats.derived.semiauto.show

  def apply(serviceParams: ServiceParams, name: String, retryPolicy: String): ActionParams =
    ActionParams(
      name = name,
      importance = Importance.Medium,
      isCounting = false,
      isTiming = false,
      retryPolicy = retryPolicy,
      serviceParams = serviceParams
    )
}

sealed private[guard] trait ActionConfigF[X]

private object ActionConfigF {
  implicit val functorActionConfigF: Functor[ActionConfigF] = cats.derived.semiauto.functor[ActionConfigF]

  final case class InitParams[K](serviceParams: ServiceParams) extends ActionConfigF[K]

  final case class WithImportance[K](value: Importance, cont: K) extends ActionConfigF[K]
  final case class WithTiming[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithCounting[K](value: Boolean, cont: K) extends ActionConfigF[K]

  def algebra(name: String, retryPolicy: String): Algebra[ActionConfigF, ActionParams] =
    Algebra[ActionConfigF, ActionParams] {
      case InitParams(sp)       => ActionParams(sp, name, retryPolicy)
      case WithImportance(v, c) => ActionParams.importance.set(v)(c)
      case WithTiming(v, c)     => ActionParams.isTiming.set(v)(c)
      case WithCounting(v, c)   => ActionParams.isCounting.set(v)(c)
    }
}

final private[guard] case class ActionConfig private (value: Fix[ActionConfigF]) {
  import ActionConfigF.*

  def trivial: ActionConfig  = ActionConfig(Fix(WithImportance(Importance.Low, value)))
  def silent: ActionConfig   = ActionConfig(Fix(WithImportance(Importance.Medium, value)))
  def notice: ActionConfig   = ActionConfig(Fix(WithImportance(Importance.High, value)))
  def critical: ActionConfig = ActionConfig(Fix(WithImportance(Importance.Critical, value)))

  def withCounting: ActionConfig    = ActionConfig(Fix(WithCounting(value = true, value)))
  def withTiming: ActionConfig      = ActionConfig(Fix(WithTiming(value = true, value)))
  def withoutCounting: ActionConfig = ActionConfig(Fix(WithCounting(value = false, value)))
  def withoutTiming: ActionConfig   = ActionConfig(Fix(WithTiming(value = false, value)))

  def evalConfig(name: String, retryPolicy: String): ActionParams =
    scheme.cata(algebra(name, retryPolicy)).apply(value)
}

private[guard] object ActionConfig {

  def apply(sp: ServiceParams): ActionConfig =
    ActionConfig(Fix(ActionConfigF.InitParams[Fix[ActionConfigF]](sp)))
}
