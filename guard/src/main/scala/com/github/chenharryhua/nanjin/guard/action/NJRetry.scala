package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.Temporal
import cats.effect.kernel.{Outcome, Ref}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.service.ServiceStatus
import fs2.concurrent.Channel
import io.circe.{Encoder, Json}
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.{Duration, ZonedDateTime}

// https://www.microsoft.com/en-us/research/wp-content/uploads/2016/07/asynch-exns.pdf
final class NJRetry[F[_], A, B] private[guard] (
  serviceStatus: Ref[F, ServiceStatus],
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionParams: ActionParams,
  arrow: A => F[B],
  transInput: A => F[Json],
  transOutput: B => F[Json],
  isWorthRetry: Kleisli[F, Throwable, Boolean])(implicit F: Temporal[F]) {
  private def copy(
    transInput: A => F[Json] = transInput,
    transOutput: B => F[Json] = transOutput,
    isWorthRetry: Kleisli[F, Throwable, Boolean] = isWorthRetry): NJRetry[F, A, B] =
    new NJRetry[F, A, B](
      serviceStatus,
      metricRegistry,
      channel,
      actionParams,
      arrow,
      transInput,
      transOutput,
      isWorthRetry)

  def withWorthRetryM(f: Throwable => F[Boolean]): NJRetry[F, A, B] = copy(isWorthRetry = Kleisli(f))
  def withWorthRetry(f: Throwable => Boolean): NJRetry[F, A, B] = withWorthRetryM(Kleisli.fromFunction(f).run)

  def logInputM(f: A => F[Json]): NJRetry[F, A, B]        = copy(transInput = f)
  def logInput(implicit ev: Encoder[A]): NJRetry[F, A, B] = logInputM((a: A) => F.pure(ev(a)))

  def logOutputM(f: B => F[Json]): NJRetry[F, A, B]        = copy(transOutput = f)
  def logOutput(implicit ev: Encoder[B]): NJRetry[F, A, B] = logOutputM((b: B) => F.pure(ev(b)))

  private[this] lazy val failCounter: Counter = metricRegistry.counter(actionFailMRName(actionParams))
  private[this] lazy val succCounter: Counter = metricRegistry.counter(actionSuccMRName(actionParams))
  private[this] lazy val timer: Timer         = metricRegistry.timer(actionTimerMRName(actionParams))

  private[this] def timingAndCounting(
    isSucc: Boolean,
    launchTime: ZonedDateTime,
    now: ZonedDateTime): Unit = {
    if (actionParams.isTiming) timer.update(Duration.between(launchTime, now))
    if (actionParams.isCounting) {
      if (isSucc) succCounter.inc(1) else failCounter.inc(1)
    }
  }

  def run(input: A): F[B] = for {
    publisher <- F
      .ref(0)
      .map(retryCounter => new ActionEventPublisher[F](serviceStatus, channel, retryCounter))
    actionInfo <- publisher.actionStart(actionParams, transInput(input))
    res <- retry.mtl
      .retryingOnSomeErrors[B]
      .apply[F, Throwable](
        actionParams.retry.policy[F],
        isWorthRetry.run,
        (error, details) =>
          details match {
            case wdr: WillDelayAndRetry => publisher.actionRetry(actionInfo, wdr, error)
            case _: GivingUp            => F.unit
          }
      )(arrow(input))
      .guaranteeCase {
        case Outcome.Canceled() =>
          publisher
            .actionFail(actionInfo, ActionException.ActionCanceled, transInput(input))
            .map(ts => timingAndCounting(isSucc = false, actionInfo.launchTime, ts))
        case Outcome.Errored(error) =>
          publisher
            .actionFail(actionInfo, error, transInput(input))
            .map(ts => timingAndCounting(isSucc = false, actionInfo.launchTime, ts))
        case Outcome.Succeeded(output) =>
          publisher
            .actionSucc(actionInfo, output.flatMap(transOutput))
            .map(ts => timingAndCounting(isSucc = true, actionInfo.launchTime, ts))
      }
  } yield res
}

final class NJRetryUnit[F[_], B] private[guard] (
  serviceStatus: Ref[F, ServiceStatus],
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionParams: ActionParams,
  fb: F[B],
  transInput: F[Json],
  transOutput: B => F[Json],
  isWorthRetry: Kleisli[F, Throwable, Boolean])(implicit F: Temporal[F]) {
  private def copy(
    transInput: F[Json] = transInput,
    transOutput: B => F[Json] = transOutput,
    isWorthRetry: Kleisli[F, Throwable, Boolean] = isWorthRetry): NJRetryUnit[F, B] =
    new NJRetryUnit[F, B](
      serviceStatus,
      metricRegistry,
      channel,
      actionParams,
      fb,
      transInput,
      transOutput,
      isWorthRetry)

  def withWorthRetryM(f: Throwable => F[Boolean]): NJRetryUnit[F, B] = copy(isWorthRetry = Kleisli(f))
  def withWorthRetry(f: Throwable => Boolean): NJRetryUnit[F, B] = withWorthRetryM(
    Kleisli.fromFunction(f).run)

  def logInputM(info: F[Json]): NJRetryUnit[F, B] = copy(transInput = info)
  def logInput(info: Json): NJRetryUnit[F, B]     = logInputM(F.pure(info))

  def logOutputM(f: B => F[Json]): NJRetryUnit[F, B]        = copy(transOutput = f)
  def logOutput(implicit ev: Encoder[B]): NJRetryUnit[F, B] = logOutputM((b: B) => F.pure(ev(b)))

  val run: F[B] =
    new NJRetry[F, Unit, B](
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionParams,
      arrow = _ => fb,
      transInput = _ => transInput,
      transOutput = transOutput,
      isWorthRetry = isWorthRetry
    ).run(())

}
