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
final class NJRetry[F[_], IN, OUT] private[guard] (
  serviceStatus: Ref[F, ServiceStatus],
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
    isWorthRetry: Kleisli[F, Throwable, Boolean] = isWorthRetry): NJRetry[F, IN, OUT] =
    new NJRetry[F, IN, OUT](
      serviceStatus,
      metricRegistry,
      channel,
      actionParams,
      arrow,
      transInput,
      transOutput,
      isWorthRetry)

  def withWorthRetryM(f: Throwable => F[Boolean]): NJRetry[F, IN, OUT] = copy(isWorthRetry = Kleisli(f))
  def withWorthRetry(f: Throwable => Boolean): NJRetry[F, IN, OUT] = withWorthRetryM(
    Kleisli.fromFunction(f).run)

  def logInputM(f: IN => F[Json]): NJRetry[F, IN, OUT]        = copy(transInput = f)
  def logInput(implicit ev: Encoder[IN]): NJRetry[F, IN, OUT] = logInputM((a: IN) => F.pure(ev(a)))

  def logOutputM(f: (IN, OUT) => F[Json]): NJRetry[F, IN, OUT] = copy(transOutput = f)
  def logOutput(f: (IN, OUT) => Json): NJRetry[F, IN, OUT] = logOutputM((a: IN, b: OUT) => F.pure(f(a, b)))
  def logOutput(implicit ev: Encoder[OUT]): NJRetry[F, IN, OUT] = logOutputM((_, b: OUT) => F.pure(ev(b)))

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

  def run(input: IN): F[OUT] = for {
    publisher <- F
      .ref(0)
      .map(retryCounter => new ActionEventPublisher[F](serviceStatus, channel, retryCounter))
    actionInfo <- publisher.actionStart(actionParams, transInput(input))
    res <- retry.mtl
      .retryingOnSomeErrors[OUT]
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
            .actionSucc(actionInfo, output.flatMap(transOutput(input, _)))
            .map(ts => timingAndCounting(isSucc = true, actionInfo.launchTime, ts))
      }
  } yield res
}

final class NJRetryUnit[F[_], OUT] private[guard] (
  serviceStatus: Ref[F, ServiceStatus],
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
    isWorthRetry: Kleisli[F, Throwable, Boolean] = isWorthRetry): NJRetryUnit[F, OUT] =
    new NJRetryUnit[F, OUT](
      serviceStatus,
      metricRegistry,
      channel,
      actionParams,
      arrow,
      transInput,
      transOutput,
      isWorthRetry)

  def withWorthRetryM(f: Throwable => F[Boolean]): NJRetryUnit[F, OUT] = copy(isWorthRetry = Kleisli(f))
  def withWorthRetry(f: Throwable => Boolean): NJRetryUnit[F, OUT] = withWorthRetryM(
    Kleisli.fromFunction(f).run)

  def logInputM(info: F[Json]): NJRetryUnit[F, OUT] = copy(transInput = info)
  def logInput(info: Json): NJRetryUnit[F, OUT]     = logInputM(F.pure(info))

  def logOutputM(f: OUT => F[Json]): NJRetryUnit[F, OUT]        = copy(transOutput = f)
  def logOutput(implicit ev: Encoder[OUT]): NJRetryUnit[F, OUT] = logOutputM((b: OUT) => F.pure(ev(b)))

  val run: F[OUT] =
    new NJRetry[F, Unit, OUT](
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionParams,
      arrow = _ => arrow,
      transInput = _ => transInput,
      transOutput = (_, b: OUT) => transOutput(b),
      isWorthRetry = isWorthRetry
    ).run(())
}
