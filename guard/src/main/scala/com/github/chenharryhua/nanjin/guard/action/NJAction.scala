package com.github.chenharryhua.nanjin.guard.action

import cats.effect.Temporal
import cats.effect.kernel.Async
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ActionStart
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
  transError: IN => F[Json],
  transOutput: (IN, OUT) => F[Json],
  isWorthRetry: Throwable => F[Boolean])(implicit F: Temporal[F]) { self =>
  private def copy(
    transError: IN => F[Json] = self.transError,
    transOutput: (IN, OUT) => F[Json] = self.transOutput,
    isWorthRetry: Throwable => F[Boolean] = self.isWorthRetry): NJAction[F, IN, OUT] =
    new NJAction[F, IN, OUT](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionParams,
      retryPolicy = retryPolicy,
      arrow = arrow,
      transError = transError,
      transOutput = transOutput,
      isWorthRetry = isWorthRetry
    )

  def withWorthRetryM(f: Throwable => F[Boolean]): NJAction[F, IN, OUT] = copy(isWorthRetry = f)
  def withWorthRetry(f: Throwable => Boolean): NJAction[F, IN, OUT] =
    withWorthRetryM((ex: Throwable) => F.pure(f(ex)))

  def logErrorM(f: IN => F[Json]): NJAction[F, IN, OUT] = copy(transError = f)
  def logError(f: IN => Json): NJAction[F, IN, OUT]     = logErrorM((a: IN) => F.pure(f(a)))

  def logOutputM(f: (IN, OUT) => F[Json]): NJAction[F, IN, OUT] = copy(transOutput = f)
  def logOutput(f: (IN, OUT) => Json): NJAction[F, IN, OUT]     = logOutputM((a, b) => F.pure(f(a, b)))

  private lazy val measures: Measures = Measures(actionParams, metricRegistry)

  private def internal(input: IN, traceInfo: Option[TraceInfo]): F[OUT] =
    for {
      ts <- actionParams.serviceParams.zonedNow
      ai <- traceInfo match {
        case Some(ti) => F.pure(ActionInfo(actionParams, ti.spanId, traceInfo, ts))
        case None     => F.unique.map(token => ActionInfo(actionParams, token.hash.toString, traceInfo, ts))
      }
      _ <- F.whenA(actionParams.importance.isPublishActionStart)(channel.send(ActionStart(ai)))
      out <- new ReTry[F, IN, OUT](
        channel = channel,
        retryPolicy = retryPolicy,
        arrow = arrow,
        transError = transError,
        transOutput = transOutput,
        isWorthRetry = isWorthRetry,
        actionInfo = ai,
        input = input,
        measures = measures
      ).execute
    } yield out

  def run(input: IN): F[OUT] = internal(input, None)

  private lazy val traceTags: List[(String, TraceValue)] = List(
    "service_id" -> TraceValue.StringValue(actionParams.serviceParams.serviceId.show),
    "digest" -> TraceValue.StringValue(actionParams.metricID.metricName.digest.value),
    "importance" -> TraceValue.StringValue(actionParams.importance.entryName)
  )

  def runWithSpan(input: IN)(span: Span[F]): F[OUT] =
    span.span(actionParams.metricID.metricName.value).use { sub =>
      for {
        _ <- sub.put(traceTags*)
        ti <- TraceInfo(sub)
        out <- internal(input, ti)
      } yield out
    }
}

final class NJAction0[F[_], OUT] private[guard] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionParams: ActionParams,
  retryPolicy: RetryPolicy[F],
  arrow: F[OUT],
  transError: F[Json],
  transOutput: OUT => F[Json],
  isWorthRetry: Throwable => F[Boolean])(implicit F: Async[F]) { self =>
  private def copy(
    transError: F[Json] = self.transError,
    transOutput: OUT => F[Json] = self.transOutput,
    isWorthRetry: Throwable => F[Boolean] = self.isWorthRetry): NJAction0[F, OUT] =
    new NJAction0[F, OUT](
      metricRegistry,
      channel = channel,
      actionParams = actionParams,
      retryPolicy = retryPolicy,
      arrow = arrow,
      transError = transError,
      transOutput = transOutput,
      isWorthRetry = isWorthRetry
    )

  def withWorthRetryM(f: Throwable => F[Boolean]): NJAction0[F, OUT] = copy(isWorthRetry = f)
  def withWorthRetry(f: Throwable => Boolean): NJAction0[F, OUT] =
    withWorthRetryM((ex: Throwable) => F.pure(f(ex)))

  def logErrorM(info: F[Json]): NJAction0[F, OUT] = copy(transError = info)
  def logError(info: => Json): NJAction0[F, OUT]  = logErrorM(F.delay(info))

  def logOutputM(f: OUT => F[Json]): NJAction0[F, OUT] = copy(transOutput = f)
  def logOutput(f: OUT => Json): NJAction0[F, OUT]     = logOutputM((b: OUT) => F.pure(f(b)))

  private lazy val njAction = new NJAction[F, Unit, OUT](
    metricRegistry = metricRegistry,
    channel = channel,
    actionParams = actionParams,
    retryPolicy = retryPolicy,
    arrow = _ => arrow,
    transError = _ => transError,
    transOutput = (_, b: OUT) => transOutput(b),
    isWorthRetry = isWorthRetry
  )

  def run: F[OUT]                        = njAction.run(())
  def runWithSpan(span: Span[F]): F[OUT] = njAction.runWithSpan(())(span)
}
