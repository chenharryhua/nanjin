package com.github.chenharryhua.nanjin.guard.config

import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.datetime.instances.*
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import monocle.macros.Lenses

import scala.concurrent.duration.*

@Lenses @JsonCodec final case class AgentParams private (
  spans: List[String],
  importance: Importance,
  isTerminate: ActionTermination,
  isCounting: CountAction,
  isTiming: TimeAction,
  retry: ActionRetryParams,
  alias: String)

private[guard] object AgentParams {
  implicit val showAgentParams: Show[AgentParams] = cats.derived.semiauto.show[AgentParams]

  def apply(): AgentParams = AgentParams(
    spans = Nil,
    importance = Importance.Medium,
    isTerminate = ActionTermination.Yes,
    isCounting = CountAction.Yes,
    isTiming = TimeAction.Yes,
    retry = ActionRetryParams(
      maxRetries = 0,
      capDelay = None,
      njRetryPolicy = NJRetryPolicy.ConstantDelay(10.seconds)
    ),
    alias = "action"
  )
}

sealed private[guard] trait AgentConfigF[F]

private object AgentConfigF {
  implicit val functorActionConfigF: Functor[AgentConfigF] = cats.derived.semiauto.functor[AgentConfigF]

  final case class InitParams[K]() extends AgentConfigF[K]

  final case class WithMaxRetries[K](value: Int, cont: K) extends AgentConfigF[K]
  final case class WithCapDelay[K](value: FiniteDuration, cont: K) extends AgentConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends AgentConfigF[K]
  final case class WithTermination[K](value: ActionTermination, cont: K) extends AgentConfigF[K]

  final case class WithSpans[K](value: List[String], cont: K) extends AgentConfigF[K]

  final case class WithImportance[K](value: Importance, cont: K) extends AgentConfigF[K]
  final case class WithTiming[K](value: TimeAction, cont: K) extends AgentConfigF[K]
  final case class WithCounting[K](value: CountAction, cont: K) extends AgentConfigF[K]

  final case class WithAlias[K](value: String, cont: K) extends AgentConfigF[K]

  val algebra: Algebra[AgentConfigF, AgentParams] =
    Algebra[AgentConfigF, AgentParams] {
      case InitParams()          => AgentParams()
      case WithRetryPolicy(v, c) => AgentParams.retry.composeLens(ActionRetryParams.njRetryPolicy).set(v)(c)
      case WithMaxRetries(v, c)  => AgentParams.retry.composeLens(ActionRetryParams.maxRetries).set(v)(c)
      case WithCapDelay(v, c)    => AgentParams.retry.composeLens(ActionRetryParams.capDelay).set(Some(v))(c)
      case WithTermination(v, c) => AgentParams.isTerminate.set(v)(c)
      case WithImportance(v, c)  => AgentParams.importance.set(v)(c)
      case WithSpans(v, c)       => AgentParams.spans.modify(_ ::: v)(c)
      case WithTiming(v, c)      => AgentParams.isTiming.set(v)(c)
      case WithCounting(v, c)    => AgentParams.isCounting.set(v)(c)
      case WithAlias(v, c)       => AgentParams.alias.set(v)(c)
    }
}

final case class AgentConfig private (value: Fix[AgentConfigF]) {
  import AgentConfigF.*

  def withMaxRetries(num: Int): AgentConfig          = AgentConfig(Fix(WithMaxRetries(num, value)))
  def withCapDelay(dur: FiniteDuration): AgentConfig = AgentConfig(Fix(WithCapDelay(dur, value)))

  def withConstantDelay(delay: FiniteDuration): AgentConfig =
    AgentConfig(Fix(WithRetryPolicy(NJRetryPolicy.ConstantDelay(delay), value)))

  def withExponentialBackoff(delay: FiniteDuration): AgentConfig =
    AgentConfig(Fix(WithRetryPolicy(NJRetryPolicy.ExponentialBackoff(delay), value)))

  def withFibonacciBackoff(delay: FiniteDuration): AgentConfig =
    AgentConfig(Fix(WithRetryPolicy(NJRetryPolicy.FibonacciBackoff(delay), value)))

  def withFullJitterBackoff(delay: FiniteDuration): AgentConfig =
    AgentConfig(Fix(WithRetryPolicy(NJRetryPolicy.FullJitter(delay), value)))

  def withNonTermination: AgentConfig =
    AgentConfig(Fix(WithTermination(value = ActionTermination.No, value)))

  def withLowImportance: AgentConfig      = AgentConfig(Fix(WithImportance(Importance.Low, value)))
  def withMediumImportance: AgentConfig   = AgentConfig(Fix(WithImportance(Importance.Medium, value)))
  def withHighImportance: AgentConfig     = AgentConfig(Fix(WithImportance(Importance.High, value)))
  def withCriticalImportance: AgentConfig = AgentConfig(Fix(WithImportance(Importance.Critical, value)))

  def withCounting: AgentConfig    = AgentConfig(Fix(WithCounting(value = CountAction.Yes, value)))
  def withTiming: AgentConfig      = AgentConfig(Fix(WithTiming(value = TimeAction.Yes, value)))
  def withoutCounting: AgentConfig = AgentConfig(Fix(WithCounting(value = CountAction.No, value)))
  def withoutTiming: AgentConfig   = AgentConfig(Fix(WithTiming(value = TimeAction.No, value)))

  def withSpan(name: String): AgentConfig = AgentConfig(Fix(WithSpans(List(name), value)))

  def withAlias(alias: String): AgentConfig = AgentConfig(Fix(WithAlias(alias, value)))

  def evalConfig: AgentParams = scheme.cata(algebra).apply(value)
}

private[guard] object AgentConfig {

  def apply(): AgentConfig = AgentConfig(Fix(AgentConfigF.InitParams[Fix[AgentConfigF]]()))
}
