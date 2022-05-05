package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.Temporal
import cats.effect.kernel.{Outcome, Ref}
import cats.effect.std.UUIDGen
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.{Duration, ZonedDateTime}

// https://www.microsoft.com/en-us/research/wp-content/uploads/2016/07/asynch-exns.pdf
final class NJRetry[F[_]: UUIDGen, A, B] private[guard] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  ongoings: Ref[F, Set[ActionInfo]],
  actionParams: ActionParams,
  kfab: Kleisli[F, A, B],
  succ: Option[Kleisli[F, (A, B), String]],
  fail: Kleisli[F, (A, Throwable), String],
  isWorthRetry: Kleisli[F, Throwable, Boolean])(implicit F: Temporal[F]) {
  private def copy(
    succ: Option[Kleisli[F, (A, B), String]] = succ,
    fail: Kleisli[F, (A, Throwable), String] = fail,
    isWorthRetry: Kleisli[F, Throwable, Boolean] = isWorthRetry): NJRetry[F, A, B] =
    new NJRetry[F, A, B](metricRegistry, channel, ongoings, actionParams, kfab, succ, fail, isWorthRetry)

  def withSuccNotesM(f: (A, B) => F[String]): NJRetry[F, A, B] = copy(succ = Some(Kleisli(f.tupled)))
  def withSuccNotes(f: (A, B) => String): NJRetry[F, A, B]     = withSuccNotesM((a, b) => F.pure(f(a, b)))

  def withFailNotesM(f: (A, Throwable) => F[String]): NJRetry[F, A, B] = copy(fail = Kleisli(f.tupled))
  def withFailNotes(f: (A, Throwable) => String): NJRetry[F, A, B]     = withFailNotesM((a, ex) => F.pure(f(a, ex)))

  def withWorthRetryM(f: Throwable => F[Boolean]): NJRetry[F, A, B] = copy(isWorthRetry = Kleisli(f))
  def withWorthRetry(f: Throwable => Boolean): NJRetry[F, A, B]     = withWorthRetryM(Kleisli.fromFunction(f).run)

  private[this] lazy val failCounter: Counter = metricRegistry.counter(actionFailMRName(actionParams))
  private[this] lazy val succCounter: Counter = metricRegistry.counter(actionSuccMRName(actionParams))
  private[this] lazy val timer: Timer         = metricRegistry.timer(actionTimerMRName(actionParams))

  private[this] def timingAndCounting(isSucc: Boolean, launchTime: ZonedDateTime, now: ZonedDateTime): Unit = {
    if (actionParams.isTiming.value) timer.update(Duration.between(launchTime, now))
    if (actionParams.isCounting.value) {
      if (isSucc) succCounter.inc(1) else failCounter.inc(1)
    }
  }

  private[this] val succNotes: (A, F[B]) => F[Notes] =
    (a: A, b: F[B]) => succ.fold(F.pure(Notes.empty))(k => b.flatMap(k.run(a, _)).map(Notes(_)))

  private[this] val failNotes: (A, Throwable) => F[Notes] =
    (a: A, ex: Throwable) => fail.run((a, ex)).map(Notes(_))

  private[this] val publisher: ActionEventPublisher[F] =
    new ActionEventPublisher[F](actionParams.serviceParams, channel, ongoings)

  def run(input: A): F[B] = for {
    retryCount <- F.ref(0) // hold number of retries
    actionInfo <- publisher.actionStart(actionParams)
    res <- retry.mtl
      .retryingOnSomeErrors[B]
      .apply[F, Throwable](
        actionParams.retry.policy[F],
        isWorthRetry.run,
        (error, details) =>
          details match {
            case wdr: WillDelayAndRetry => publisher.actionRetry(actionInfo, retryCount, wdr, error)
            case _: GivingUp            => F.unit
          }
      )(kfab.run(input))
      .guaranteeCase {
        case Outcome.Canceled() =>
          publisher
            .actionFail[A](actionInfo, retryCount, ActionException.ActionCanceled, input, failNotes)
            .map(ts => timingAndCounting(isSucc = false, actionInfo.launchTime, ts))
        case Outcome.Errored(error) =>
          publisher
            .actionFail[A](actionInfo, retryCount, error, input, failNotes)
            .map(ts => timingAndCounting(isSucc = false, actionInfo.launchTime, ts))
        case Outcome.Succeeded(output) =>
          publisher
            .actionSucc[A, B](actionInfo, retryCount, input, output, succNotes)
            .map(ts => timingAndCounting(isSucc = true, actionInfo.launchTime, ts))
      }
  } yield res
}

final class NJRetryUnit[F[_]: Temporal: UUIDGen, B] private[guard] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  ongoings: Ref[F, Set[ActionInfo]],
  actionParams: ActionParams,
  fb: F[B],
  succ: Option[Kleisli[F, B, String]],
  fail: Kleisli[F, Throwable, String],
  isWorthRetry: Kleisli[F, Throwable, Boolean]) {
  private def copy(
    succ: Option[Kleisli[F, B, String]] = succ,
    fail: Kleisli[F, Throwable, String] = fail,
    isWorthRetry: Kleisli[F, Throwable, Boolean] = isWorthRetry): NJRetryUnit[F, B] =
    new NJRetryUnit[F, B](metricRegistry, channel, ongoings, actionParams, fb, succ, fail, isWorthRetry)

  def withSuccNotesM(f: B => F[String]): NJRetryUnit[F, B] = copy(succ = Some(Kleisli(f)))
  def withSuccNotes(f: B => String): NJRetryUnit[F, B]     = withSuccNotesM(Kleisli.fromFunction(f).run)

  def withFailNotesM(f: Throwable => F[String]): NJRetryUnit[F, B] = copy(fail = Kleisli(f))
  def withFailNotes(f: Throwable => String): NJRetryUnit[F, B]     = withFailNotesM(Kleisli.fromFunction(f).run)

  def withWorthRetryM(f: Throwable => F[Boolean]): NJRetryUnit[F, B] = copy(isWorthRetry = Kleisli(f))
  def withWorthRetry(f: Throwable => Boolean): NJRetryUnit[F, B]     = withWorthRetryM(Kleisli.fromFunction(f).run)

  val run: F[B] =
    new NJRetry[F, Unit, B](
      metricRegistry: MetricRegistry,
      channel: Channel[F, NJEvent],
      ongoings: Ref[F, Set[ActionInfo]],
      actionParams = actionParams,
      kfab = Kleisli(_ => fb),
      succ = succ.map(_.local(_._2)),
      fail = fail.local(_._2),
      isWorthRetry = isWorthRetry
    ).run(())
}
