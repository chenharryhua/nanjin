package com.github.chenharryhua.nanjin.guard.action

import cats.effect.Temporal
import cats.effect.kernel.Async
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import io.circe.{Encoder, Json}
import natchez.{Span, Trace}
import retry.RetryPolicy

// https://www.microsoft.com/en-us/research/wp-content/uploads/2016/07/asynch-exns.pdf
final class NJAction[F[_], IN, OUT] private[action] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionParams: ActionParams,
  retryPolicy: RetryPolicy[F],
  arrow: IN => F[OUT],
  transInput: IN => F[Json],
  transOutput: (IN, OUT) => F[Json],
  isWorthRetry: Throwable => F[Boolean])(implicit F: Temporal[F]) {
  private def copy(
    transInput: IN => F[Json] = transInput,
    transOutput: (IN, OUT) => F[Json] = transOutput,
    isWorthRetry: Throwable => F[Boolean] = isWorthRetry): NJAction[F, IN, OUT] =
    new NJAction[F, IN, OUT](
      metricRegistry,
      channel,
      actionParams,
      retryPolicy,
      arrow,
      transInput,
      transOutput,
      isWorthRetry)

  def withWorthRetryM(f: Throwable => F[Boolean]): NJAction[F, IN, OUT] = copy(isWorthRetry = f)
  def withWorthRetry(f: Throwable => Boolean): NJAction[F, IN, OUT] =
    withWorthRetryM((ex: Throwable) => F.pure(f(ex)))

  def logInputM(f: IN => F[Json]): NJAction[F, IN, OUT]            = copy(transInput = f)
  def logInput(implicit encode: Encoder[IN]): NJAction[F, IN, OUT] = logInputM((a: IN) => F.pure(encode(a)))

  def logOutputM(f: (IN, OUT) => F[Json]): NJAction[F, IN, OUT] = copy(transOutput = f)
  def logOutput(f: (IN, OUT) => Json): NJAction[F, IN, OUT]     = logOutputM((a, b) => F.pure(f(a, b)))

  private val failCounter: Counter = metricRegistry.counter(actionFailMRName(actionParams))
  private val succCounter: Counter = metricRegistry.counter(actionSuccMRName(actionParams))
  private val timer: Timer         = metricRegistry.timer(actionTimerMRName(actionParams))

  private def internal(input: IN, traceInfo: Option[TraceInfo]): F[OUT] =
    for {
      ts <- actionParams.serviceParams.zonedNow
      ai <- F.unique.map(token => ActionInfo(actionParams, token.hash, traceInfo, ts))
      out <- new ReTry[F, IN, OUT](
        channel = channel,
        retryPolicy = retryPolicy,
        arrow = arrow,
        transInput = transInput,
        transOutput = transOutput,
        isWorthRetry = isWorthRetry,
        failCounter = failCounter,
        succCounter = succCounter,
        timer = timer,
        actionInfo = ai,
        input = input
      ).execute
    } yield out

  def run(input: IN): F[OUT] = internal(input, None)

  def runWithTrace(input: IN)(implicit trace: Trace[F]): F[OUT] =
    for {
      _ <- trace.put("nj_action" -> actionParams.digested.metricRepr)
      ti <- TraceInfo(trace)
      out <- internal(input, Some(ti))
    } yield out

  def runWithSpan(input: IN)(span: Span[F]): F[OUT] =
    for {
      _ <- span.put("nj_action" -> actionParams.digested.metricRepr)
      ti <- TraceInfo(span)
      out <- internal(input, Some(ti))
    } yield out

}

final class NJAction0[F[_], OUT] private[guard] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionParams: ActionParams,
  retryPolicy: RetryPolicy[F],
  arrow: F[OUT],
  transInput: F[Json],
  transOutput: OUT => F[Json],
  isWorthRetry: Throwable => F[Boolean])(implicit F: Async[F]) {
  private def copy(
    transInput: F[Json] = transInput,
    transOutput: OUT => F[Json] = transOutput,
    isWorthRetry: Throwable => F[Boolean] = isWorthRetry): NJAction0[F, OUT] =
    new NJAction0[F, OUT](
      metricRegistry,
      channel,
      actionParams,
      retryPolicy,
      arrow,
      transInput,
      transOutput,
      isWorthRetry)

  def withWorthRetryM(f: Throwable => F[Boolean]): NJAction0[F, OUT] = copy(isWorthRetry = f)
  def withWorthRetry(f: Throwable => Boolean): NJAction0[F, OUT] =
    withWorthRetryM((ex: Throwable) => F.pure(f(ex)))

  def logInputM(info: F[Json]): NJAction0[F, OUT] = copy(transInput = info)
  def logInput(info: => Json): NJAction0[F, OUT]  = logInputM(F.delay(info))

  def logOutputM(f: OUT => F[Json]): NJAction0[F, OUT]            = copy(transOutput = f)
  def logOutput(implicit encode: Encoder[OUT]): NJAction0[F, OUT] = logOutputM((b: OUT) => F.pure(encode(b)))

  private lazy val njAction = new NJAction[F, Unit, OUT](
    metricRegistry = metricRegistry,
    channel = channel,
    actionParams = actionParams,
    retryPolicy = retryPolicy,
    arrow = _ => arrow,
    transInput = _ => transInput,
    transOutput = (_, b: OUT) => transOutput(b),
    isWorthRetry = isWorthRetry
  )

  val run: F[OUT]                                    = njAction.run(())
  def runWithSpan(span: Span[F]): F[OUT]             = njAction.runWithSpan(())(span)
  def runWithTrace(implicit trace: Trace[F]): F[OUT] = njAction.runWithTrace(())
}
