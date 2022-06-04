package com.github.chenharryhua.nanjin.guard.config

import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.guard.{MaxRetry, Span}
import com.github.chenharryhua.nanjin.datetime.instances.*
import eu.timepit.refined.cats.*
import eu.timepit.refined.refineMV
import higherkindness.droste.{scheme, Algebra}
import higherkindness.droste.data.Fix
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import io.circe.refined.*
import monocle.macros.Lenses

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

@Lenses @JsonCodec final case class AgentParams private (
  spans: List[Span],
  importance: Importance,
  isCounting: Boolean, // if counting the action?
  isTiming: Boolean, // if timing the action?
  isExpensive: Boolean, // if the action take long time to accomplish, like a few minutes or hours?
  retry: ActionRetryParams,
  serviceParams: ServiceParams)

private[guard] object AgentParams {
  implicit val showAgentParams: Show[AgentParams] = cats.derived.semiauto.show[AgentParams]

  def apply(serviceParams: ServiceParams): AgentParams = AgentParams(
    spans = Nil,
    importance = Importance.Medium,
    isCounting = false,
    isTiming = false,
    isExpensive = false,
    retry = ActionRetryParams(
      maxRetries = refineMV(0),
      capDelay = None,
      njRetryPolicy = NJRetryPolicy.ConstantDelay(Duration.ofSeconds(10)) // 10 seconds by default
    ),
    serviceParams = serviceParams
  )
}

sealed private[guard] trait AgentConfigF[X]

private object AgentConfigF {
  implicit val functorActionConfigF: Functor[AgentConfigF] = cats.derived.semiauto.functor[AgentConfigF]

  final case class InitParams[K](serviceParams: ServiceParams) extends AgentConfigF[K]

  final case class WithMaxRetries[K](value: MaxRetry, cont: K) extends AgentConfigF[K]
  final case class WithCapDelay[K](value: Duration, cont: K) extends AgentConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends AgentConfigF[K]

  final case class WithSpans[K](value: List[Span], cont: K) extends AgentConfigF[K]

  final case class WithImportance[K](value: Importance, cont: K) extends AgentConfigF[K]
  final case class WithTiming[K](value: Boolean, cont: K) extends AgentConfigF[K]
  final case class WithCounting[K](value: Boolean, cont: K) extends AgentConfigF[K]
  final case class WithExpensive[K](value: Boolean, cont: K) extends AgentConfigF[K]

  val algebra: Algebra[AgentConfigF, AgentParams] =
    Algebra[AgentConfigF, AgentParams] {
      case InitParams(sp)        => AgentParams(sp)
      case WithRetryPolicy(v, c) => AgentParams.retry.composeLens(ActionRetryParams.njRetryPolicy).set(v)(c)
      case WithMaxRetries(v, c)  => AgentParams.retry.composeLens(ActionRetryParams.maxRetries).set(v)(c)
      case WithCapDelay(v, c)    => AgentParams.retry.composeLens(ActionRetryParams.capDelay).set(Some(v))(c)
      case WithImportance(v, c)  => AgentParams.importance.set(v)(c)
      case WithSpans(v, c)       => AgentParams.spans.modify(_ ::: v)(c)
      case WithTiming(v, c)      => AgentParams.isTiming.set(v)(c)
      case WithCounting(v, c)    => AgentParams.isCounting.set(v)(c)
      case WithExpensive(v, c)   => AgentParams.isExpensive.set(v)(c)
    }
}

final case class AgentConfig private (value: Fix[AgentConfigF]) {
  import AgentConfigF.*

  def withMaxRetries(num: MaxRetry): AgentConfig     = AgentConfig(Fix(WithMaxRetries(num, value)))
  def withCapDelay(dur: FiniteDuration): AgentConfig = AgentConfig(Fix(WithCapDelay(dur.toJava, value)))

  def withConstantDelay(delay: FiniteDuration): AgentConfig =
    AgentConfig(Fix(WithRetryPolicy(NJRetryPolicy.ConstantDelay(delay.toJava), value)))

  def withExponentialBackoff(delay: FiniteDuration): AgentConfig =
    AgentConfig(Fix(WithRetryPolicy(NJRetryPolicy.ExponentialBackoff(delay.toJava), value)))

  def withFibonacciBackoff(delay: FiniteDuration): AgentConfig =
    AgentConfig(Fix(WithRetryPolicy(NJRetryPolicy.FibonacciBackoff(delay.toJava), value)))

  def withFullJitterBackoff(delay: FiniteDuration): AgentConfig =
    AgentConfig(Fix(WithRetryPolicy(NJRetryPolicy.FullJitter(delay.toJava), value)))

  def withLowImportance: AgentConfig      = AgentConfig(Fix(WithImportance(Importance.Low, value)))
  def withMediumImportance: AgentConfig   = AgentConfig(Fix(WithImportance(Importance.Medium, value)))
  def withHighImportance: AgentConfig     = AgentConfig(Fix(WithImportance(Importance.High, value)))
  def withCriticalImportance: AgentConfig = AgentConfig(Fix(WithImportance(Importance.Critical, value)))

  def withCounting: AgentConfig                     = AgentConfig(Fix(WithCounting(value = true, value)))
  def withTiming: AgentConfig                       = AgentConfig(Fix(WithTiming(value = true, value)))
  def withoutCounting: AgentConfig                  = AgentConfig(Fix(WithCounting(value = false, value)))
  def withoutTiming: AgentConfig                    = AgentConfig(Fix(WithTiming(value = false, value)))
  def withExpensive(isCostly: Boolean): AgentConfig = AgentConfig(Fix(WithExpensive(value = isCostly, value)))

  def withSpan(name: Span): AgentConfig        = AgentConfig(Fix(WithSpans(List(name), value)))
  def withSpan(spans: List[Span]): AgentConfig = AgentConfig(Fix(WithSpans(spans, value)))

  def evalConfig: AgentParams = scheme.cata(algebra).apply(value)
}

private[guard] object AgentConfig {

  def apply(sp: ServiceParams): AgentConfig = AgentConfig(Fix(AgentConfigF.InitParams[Fix[AgentConfigF]](sp)))
}
