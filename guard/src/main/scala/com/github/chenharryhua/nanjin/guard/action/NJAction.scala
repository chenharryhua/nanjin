package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.Temporal
import cats.effect.kernel.Outcome
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import io.circe.{Encoder, Json}
import natchez.Span
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.{Duration, ZonedDateTime}

// https://www.microsoft.com/en-us/research/wp-content/uploads/2016/07/asynch-exns.pdf
final class NJAction[F[_], IN, OUT] private[action] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionParams: ActionParams,
  arrow: IN => F[OUT],
  transInput: IN => F[Json],
  transOutput: (IN, OUT) => F[Json],
  isWorthRetry: Kleisli[F, Throwable, Boolean])(implicit F: Temporal[F]) {
  private def copy(
    transInput: IN => F[Json] = transInput,
    transOutput: (IN, OUT) => F[Json] = transOutput,
    isWorthRetry: Kleisli[F, Throwable, Boolean] = isWorthRetry): NJAction[F, IN, OUT] =
    new NJAction[F, IN, OUT](
      metricRegistry,
      channel,
      actionParams,
      arrow,
      transInput,
      transOutput,
      isWorthRetry)

  def withWorthRetryM(f: Throwable => F[Boolean]): NJAction[F, IN, OUT] = copy(isWorthRetry = Kleisli(f))
  def withWorthRetry(f: Throwable => Boolean): NJAction[F, IN, OUT] =
    withWorthRetryM(Kleisli.fromFunction(f).run)

  def logInputM(f: IN => F[Json]): NJAction[F, IN, OUT]        = copy(transInput = f)
  def logInput(implicit ev: Encoder[IN]): NJAction[F, IN, OUT] = logInputM((a: IN) => F.pure(ev(a)))

  def logOutputM(f: (IN, OUT) => F[Json]): NJAction[F, IN, OUT] = copy(transOutput = f)
  def logOutput(f: (IN, OUT) => Json): NJAction[F, IN, OUT]     = logOutputM((a, b) => F.pure(f(a, b)))

  private[this] def timingAndCounting(
    ai: ActionInfo,
    isSucc: Boolean,
    launchTime: ZonedDateTime,
    now: ZonedDateTime): Unit = {
    if (actionParams.isTiming)
      metricRegistry.timer(actionTimerMRName(ai)).update(Duration.between(launchTime, now))
    if (actionParams.isCounting) {
      if (isSucc) metricRegistry.counter(actionSuccMRName(ai)).inc(1)
      else metricRegistry.counter(actionFailMRName(ai)).inc(1)
    }
  }

  def apply(name: String, input: IN, span: Option[Span[F]]): F[OUT] =
    F.bracketCase(publisher.actionStart(name, channel, actionParams, transInput(input), span))(actionInfo =>
      retry.mtl
        .retryingOnSomeErrors[OUT]
        .apply[F, Throwable](
          actionParams.retry.policy[F],
          isWorthRetry.run,
          (error, details) =>
            details match {
              case wdr: WillDelayAndRetry => publisher.actionRetry(channel, actionInfo, wdr, error)
              case _: GivingUp            => F.unit
            }
        )(arrow(input))) { case (actionInfo, oc) =>
      oc match {
        case Outcome.Canceled() =>
          publisher
            .actionFail(channel, actionInfo, ActionException.ActionCanceled, transInput(input))
            .map(ts => timingAndCounting(actionInfo, isSucc = false, actionInfo.launchTime, ts))
        case Outcome.Errored(error) =>
          publisher
            .actionFail(channel, actionInfo, error, transInput(input))
            .map(ts => timingAndCounting(actionInfo, isSucc = false, actionInfo.launchTime, ts))
        case Outcome.Succeeded(output) =>
          publisher
            .actionSucc(channel, actionInfo, output.flatMap(transOutput(input, _)))
            .map(ts => timingAndCounting(actionInfo, isSucc = true, actionInfo.launchTime, ts))
      }
    }

  // def run(name: String)(input: IN): F[OUT] = apply(name, input, None)

  def run[A](a: A)(name: String)(implicit ev: A =:= IN): F[OUT] =
    apply(name, a, None)
  def run[A, B](a: A, b: B)(name: String)(implicit ev: (A, B) =:= IN): F[OUT] =
    apply(name, (a, b), None)
  def run[A, B, C](a: A, b: B, c: C)(name: String)(implicit ev: (A, B, C) =:= IN): F[OUT] =
    apply(name, (a, b, c), None)
  def run[A, B, C, D](a: A, b: B, c: C, d: D)(name: String)(implicit ev: (A, B, C, D) =:= IN): F[OUT] =
    apply(name, (a, b, c, d), None)
  def run[A, B, C, D, E](a: A, b: B, c: C, d: D, e: E)(name: String)(implicit
    ev: (A, B, C, D, E) =:= IN): F[OUT] =
    apply(name, (a, b, c, d, e), None)
}

final class NJAction0[F[_], OUT] private[guard] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionParams: ActionParams,
  arrow: F[OUT],
  transInput: F[Json],
  transOutput: OUT => F[Json],
  isWorthRetry: Kleisli[F, Throwable, Boolean])(implicit F: Temporal[F]) {
  private def copy(
    transInput: F[Json] = transInput,
    transOutput: OUT => F[Json] = transOutput,
    isWorthRetry: Kleisli[F, Throwable, Boolean] = isWorthRetry): NJAction0[F, OUT] =
    new NJAction0[F, OUT](metricRegistry, channel, actionParams, arrow, transInput, transOutput, isWorthRetry)

  def withWorthRetryM(f: Throwable => F[Boolean]): NJAction0[F, OUT] = copy(isWorthRetry = Kleisli(f))
  def withWorthRetry(f: Throwable => Boolean): NJAction0[F, OUT] = withWorthRetryM(
    Kleisli.fromFunction(f).run)

  def logInputM(info: F[Json]): NJAction0[F, OUT] = copy(transInput = info)
  def logInput(info: Json): NJAction0[F, OUT]     = logInputM(F.pure(info))

  def logOutputM(f: OUT => F[Json]): NJAction0[F, OUT]        = copy(transOutput = f)
  def logOutput(implicit ev: Encoder[OUT]): NJAction0[F, OUT] = logOutputM((b: OUT) => F.pure(ev(b)))

  private val njAction = new NJAction[F, Unit, OUT](
    metricRegistry = metricRegistry,
    channel = channel,
    actionParams = actionParams,
    arrow = _ => arrow,
    transInput = _ => transInput,
    transOutput = (_, b: OUT) => transOutput(b),
    isWorthRetry = isWorthRetry
  )

  def run(name: String): F[OUT]                = njAction.apply(name, (), None)
  def run(name: String, span: Span[F]): F[OUT] = njAction.apply(name, (), Some(span))
}
