package com.github.chenharryhua.nanjin.guard

import cats.collections.Predicate
import cats.data.{Kleisli, Reader}
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import cats.{Alternative, Traverse}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, AgentConfig, AgentParams, DigestedName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.Stream

import java.time.ZoneId

final class Agent[F[_]] private[guard] (
  publisher: EventPublisher[F],
  dispatcher: Dispatcher[F],
  agentConfig: AgentConfig)(implicit F: Async[F])
    extends UpdateConfig[AgentConfig, Agent[F]] {

  val agentParams: AgentParams     = agentConfig.evalConfig
  val serviceParams: ServiceParams = publisher.serviceParams
  val zoneId: ZoneId               = publisher.serviceParams.taskParams.zoneId
  val digestedName: DigestedName   = DigestedName(agentParams.spans, publisher.serviceParams)

  override def updateConfig(f: AgentConfig => AgentConfig): Agent[F] =
    new Agent[F](publisher, dispatcher, f(agentConfig))

  def span(name: String): Agent[F] = updateConfig(_.withSpan(name))

  def trivial: Agent[F]  = updateConfig(_.withLowImportance)
  def normal: Agent[F]   = updateConfig(_.withMediumImportance)
  def notice: Agent[F]   = updateConfig(_.withHighImportance)
  def critical: Agent[F] = updateConfig(_.withCriticalImportance)

  def retry[A, B](f: A => F[B]): NJRetry[F, A, B] =
    new NJRetry[F, A, B](
      publisher = publisher,
      params = ActionParams(agentParams),
      kfab = Kleisli(f),
      succ = Kleisli(_ => F.pure("")),
      fail = Kleisli(_ => F.pure("")),
      isWorthRetry = Reader(_ => true),
      postCondition = Predicate(_ => true))

  def retry[B](fb: F[B]): NJRetryUnit[F, B] =
    new NJRetryUnit[F, B](
      fb = fb,
      publisher = publisher,
      params = ActionParams(agentParams),
      succ = Kleisli(_ => F.pure("")),
      fail = Kleisli(_ => F.pure("")),
      isWorthRetry = Reader(_ => true),
      postCondition = Predicate(_ => true))

  def run[B](fb: F[B]): F[B]             = retry(fb).run
  def run[B](sfb: Stream[F, B]): F[Unit] = run(sfb.compile.drain)

  def broker(metricName: String): NJBroker[F] =
    new NJBroker[F](
      DigestedName(agentParams.spans :+ metricName, agentParams.serviceParams),
      dispatcher: Dispatcher[F],
      publisher: EventPublisher[F],
      isCountAsError = false)

  def alert(alertName: String): NJAlert[F] =
    new NJAlert(
      DigestedName(agentParams.spans :+ alertName, agentParams.serviceParams),
      dispatcher: Dispatcher[F],
      publisher: EventPublisher[F])

  def counter(counterName: String): NJCounter[F] =
    new NJCounter(
      DigestedName(agentParams.spans :+ counterName, agentParams.serviceParams),
      publisher.metricRegistry,
      isCountAsError = false)

  def meter(meterName: String): NJMeter[F] =
    new NJMeter[F](DigestedName(agentParams.spans :+ meterName, agentParams.serviceParams), publisher.metricRegistry)

  def histogram(metricName: String): NJHistogram[F] =
    new NJHistogram[F](
      DigestedName(agentParams.spans :+ metricName, agentParams.serviceParams),
      publisher.metricRegistry
    )

  val metrics: NJMetrics[F] = new NJMetrics[F](dispatcher, publisher)

  def runtime: NJRuntimeInfo[F] = new NJRuntimeInfo[F](publisher.serviceStatus, publisher.ongoings)

  // maximum retries
  def max(retries: Int): Agent[F] = updateConfig(_.withMaxRetries(retries))

  def nonStop[B](fb: F[B]): F[Nothing] =
    span("nonStop")
      .updateConfig(_.withNonTermination.withMaxRetries(0).withoutTiming.withoutCounting.withLowImportance)
      .retry(fb)
      .run
      .flatMap[Nothing](_ => F.raiseError(new Exception("never happen")))

  def nonStop[B](sfb: Stream[F, B]): F[Nothing] = nonStop(sfb.compile.drain)

  def quasi[T[_]: Traverse: Alternative, B](tfb: T[F[B]]): F[T[B]] =
    run(tfb.traverse(_.attempt).map(_.partitionEither(identity)).map(_._2))

  def quasi[B](fbs: F[B]*): F[List[B]] = quasi[List, B](fbs.toList)

  def quasi[T[_]: Traverse: Alternative, B](parallelism: Int, tfb: T[F[B]]): F[T[B]] =
    run(F.parTraverseN(parallelism)(tfb)(_.attempt).map(_.partitionEither(identity)).map(_._2))

  def quasi[B](parallelism: Int)(tfb: F[B]*): F[List[B]] = quasi[List, B](parallelism, tfb.toList)
}
