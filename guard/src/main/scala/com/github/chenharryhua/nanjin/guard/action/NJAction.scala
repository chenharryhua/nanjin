package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Temporal
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import io.circe.Json
import natchez.{Span, TraceValue}
import retry.RetryPolicy

// https://www.microsoft.com/en-us/research/wp-content/uploads/2016/07/asynch-exns.pdf
final class NJAction[F[_], IN, OUT] private[action] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionParams: ActionParams,
  retryPolicy: RetryPolicy[F],
  arrow: IN => F[OUT],
  transInput: IN => F[Json],
  transOutput: Option[(IN, OUT) => Json],
  isWorthRetry: Throwable => F[Boolean])(implicit F: Temporal[F]) { self =>
  private def copy(
    transInput: IN => F[Json] = self.transInput,
    transOutput: Option[(IN, OUT) => Json] = self.transOutput,
    isWorthRetry: Throwable => F[Boolean] = self.isWorthRetry): NJAction[F, IN, OUT] =
    new NJAction[F, IN, OUT](
      metricRegistry = self.metricRegistry,
      channel = self.channel,
      actionParams = self.actionParams,
      retryPolicy = self.retryPolicy,
      arrow = self.arrow,
      transInput = transInput,
      transOutput = transOutput,
      isWorthRetry = isWorthRetry
    )

  def withWorthRetryM(f: Throwable => F[Boolean]): NJAction[F, IN, OUT] = copy(isWorthRetry = f)
  def withWorthRetry(f: Throwable => Boolean): NJAction[F, IN, OUT] =
    withWorthRetryM((ex: Throwable) => F.pure(f(ex)))

  def logInputM(f: IN => F[Json]): NJAction[F, IN, OUT] = copy(transInput = f)
  def logInput(f: IN => Json): NJAction[F, IN, OUT]     = logInputM((a: IN) => F.pure(f(a)))

  def logOutput(f: (IN, OUT) => Json): NJAction[F, IN, OUT] = copy(transOutput = Some(f))

  private[this] lazy val actionRunner: ReTry[F, IN, OUT] =
    new ReTry[F, IN, OUT](
      metricRegistry = metricRegistry,
      actionParams = actionParams,
      channel = channel,
      retryPolicy = retryPolicy,
      arrow = arrow,
      transInput = transInput,
      transOutput = transOutput,
      isWorthRetry = isWorthRetry
    )

  def run(input: IN): F[OUT] = actionRunner.run(input)

  private lazy val traceTags: List[(String, TraceValue)] = List(
    "service_id" -> TraceValue.StringValue(actionParams.serviceParams.serviceId.show),
    "digest" -> TraceValue.StringValue(actionParams.metricId.metricName.digest)
  )

  def runWithSpan(input: IN)(span: Span[F]): F[OUT] =
    span.span(actionParams.metricId.metricName.value).use { sub =>
      for {
        _ <- sub.put(traceTags*)
        ti <- TraceInfo(sub)
        out <- actionRunner.run(input, ti)
      } yield out
    }
}

final class NJAction0[F[_], OUT] private[guard] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionParams: ActionParams,
  retryPolicy: RetryPolicy[F],
  arrow: F[OUT],
  transInput: F[Json],
  transOutput: Option[OUT => Json],
  isWorthRetry: Throwable => F[Boolean])(implicit F: Temporal[F]) { self =>
  private def copy(
    transInput: F[Json] = self.transInput,
    transOutput: Option[OUT => Json] = self.transOutput,
    isWorthRetry: Throwable => F[Boolean] = self.isWorthRetry): NJAction0[F, OUT] =
    new NJAction0[F, OUT](
      metricRegistry = self.metricRegistry,
      channel = self.channel,
      actionParams = self.actionParams,
      retryPolicy = self.retryPolicy,
      arrow = self.arrow,
      transInput = transInput,
      transOutput = transOutput,
      isWorthRetry = isWorthRetry
    )

  def withWorthRetryM(f: Throwable => F[Boolean]): NJAction0[F, OUT] = copy(isWorthRetry = f)
  def withWorthRetry(f: Throwable => Boolean): NJAction0[F, OUT] =
    withWorthRetryM((ex: Throwable) => F.pure(f(ex)))

  def logInputM(info: F[Json]): NJAction0[F, OUT] = copy(transInput = info)
  def logInput(info: Json): NJAction0[F, OUT]     = logInputM(F.pure(info))

  def logOutput(f: OUT => Json): NJAction0[F, OUT] = copy(transOutput = Some(f))

  private lazy val njAction = new NJAction[F, Unit, OUT](
    metricRegistry = metricRegistry,
    channel = channel,
    actionParams = actionParams,
    retryPolicy = retryPolicy,
    arrow = _ => arrow,
    transInput = _ => transInput,
    transOutput = transOutput.map(f => (_, b: OUT) => f(b)),
    isWorthRetry = isWorthRetry
  )

  def run: F[OUT]                        = njAction.run(())
  def runWithSpan(span: Span[F]): F[OUT] = njAction.runWithSpan(())(span)
}
