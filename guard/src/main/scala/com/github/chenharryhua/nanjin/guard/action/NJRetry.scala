package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, OptionT}
import cats.effect.Temporal
import cats.effect.kernel.{Outcome, Ref}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.service.ServiceStatus
import fs2.concurrent.Channel
import io.circe.Json
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.{Duration, ZonedDateTime}

// https://www.microsoft.com/en-us/research/wp-content/uploads/2016/07/asynch-exns.pdf
final class NJRetry[F[_], A, B] private[guard] (
  serviceStatus: Ref[F, ServiceStatus],
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionParams: ActionParams,
  kfab: Kleisli[F, A, B],
  transInput: OptionT[F, Kleisli[F, A, Json]],
  transOutput: OptionT[F, Kleisli[F, B, Json]],
  isWorthRetry: Kleisli[F, Throwable, Boolean])(implicit F: Temporal[F]) {
  private def copy(
    transInput: OptionT[F, Kleisli[F, A, Json]] = transInput,
    transOutput: OptionT[F, Kleisli[F, B, Json]] = transOutput,
    isWorthRetry: Kleisli[F, Throwable, Boolean] = isWorthRetry): NJRetry[F, A, B] =
    new NJRetry[F, A, B](
      serviceStatus,
      metricRegistry,
      channel,
      actionParams,
      kfab,
      transInput,
      transOutput,
      isWorthRetry)

  def withWorthRetryM(f: Throwable => F[Boolean]): NJRetry[F, A, B] = copy(isWorthRetry = Kleisli(f))
  def withWorthRetry(f: Throwable => Boolean): NJRetry[F, A, B] = withWorthRetryM(Kleisli.fromFunction(f).run)

  def withInputM(f: A => F[Json]): NJRetry[F, A, B] = copy(transInput = OptionT.liftF(F.pure(Kleisli(f))))
  def withInput(f: A => Json): NJRetry[F, A, B]     = withInputM(Kleisli.fromFunction(f).run)

  def withOutputM(f: B => F[Json]): NJRetry[F, A, B] = copy(transOutput = OptionT.liftF(F.pure(Kleisli(f))))
  def withOutput(f: B => Json): NJRetry[F, A, B]     = withOutputM(Kleisli.fromFunction(f).run)

  private[this] lazy val failCounter: Counter = metricRegistry.counter(actionFailMRName(actionParams))
  private[this] lazy val succCounter: Counter = metricRegistry.counter(actionSuccMRName(actionParams))
  private[this] lazy val timer: Timer         = metricRegistry.timer(actionTimerMRName(actionParams))

  private[this] def timingAndCounting(
    isSucc: Boolean,
    launchTime: ZonedDateTime,
    now: ZonedDateTime): Unit = {
    if (actionParams.isTiming.value) timer.update(Duration.between(launchTime, now))
    if (actionParams.isCounting.value) {
      if (isSucc) succCounter.inc(1) else failCounter.inc(1)
    }
  }

  def run(input: A): F[B] = for {
    publisher <- F
      .ref(0)
      .map(retryCounter => new ActionEventPublisher[F](serviceStatus, channel, retryCounter))
    actionInfo <- publisher.actionStart(actionParams, transInput.value.flatMap(_.traverse(_.run(input))))
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
      )(kfab.run(input))
      .guaranteeCase {
        case Outcome.Canceled() =>
          publisher
            .actionFail(actionInfo, ActionException.ActionCanceled)
            .map(ts => timingAndCounting(isSucc = false, actionInfo.launchTime, ts))
        case Outcome.Errored(error) =>
          publisher
            .actionFail(actionInfo, error)
            .map(ts => timingAndCounting(isSucc = false, actionInfo.launchTime, ts))
        case Outcome.Succeeded(output) =>
          publisher
            .actionSucc(actionInfo, output.flatMap(fb => transOutput.value.flatMap(_.traverse(_.run(fb)))))
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
  transInput: OptionT[F, Json],
  transOutput: OptionT[F, Kleisli[F, B, Json]],
  isWorthRetry: Kleisli[F, Throwable, Boolean])(implicit F: Temporal[F]) {
  private def copy(
    transInput: OptionT[F, Json] = transInput,
    transOutput: OptionT[F, Kleisli[F, B, Json]] = transOutput,
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

  def withInputM(info: F[Json]): NJRetryUnit[F, B] = copy(transInput = OptionT.liftF(info))
  def withInput(info: Json): NJRetryUnit[F, B]     = withInputM(F.pure(info))

  def withOutputM(f: B => F[Json]): NJRetryUnit[F, B] = copy(transOutput = OptionT.liftF(F.pure(Kleisli(f))))
  def withOutput(f: B => Json): NJRetryUnit[F, B]     = withOutputM(Kleisli.fromFunction(f).run)

  val run: F[B] =
    new NJRetry[F, Unit, B](
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionParams,
      kfab = Kleisli(_ => fb),
      transInput = transInput.map(js => Kleisli(_ => F.pure(js))),
      transOutput = transOutput,
      isWorthRetry = isWorthRetry
    ).run(())

}
