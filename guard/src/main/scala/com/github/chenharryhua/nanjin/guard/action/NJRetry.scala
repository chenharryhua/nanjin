package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, Reader}
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

import java.time.{Duration, Instant}

// https://www.microsoft.com/en-us/research/wp-content/uploads/2016/07/asynch-exns.pdf
final class NJRetry[F[_]: UUIDGen, A, B] private[guard] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  ongoings: Ref[F, Set[ActionInfo]],
  actionParams: ActionParams,
  kfab: Kleisli[F, A, B],
  succ: Option[Kleisli[F, (A, B), String]],
  fail: Option[Kleisli[F, (A, Throwable), String]],
  isWorthRetry: Reader[Throwable, Boolean])(implicit F: Temporal[F]) {
  private def copy(
    metricRegistry: MetricRegistry = metricRegistry,
    channel: Channel[F, NJEvent] = channel,
    ongoings: Ref[F, Set[ActionInfo]] = ongoings,
    actionParams: ActionParams = actionParams,
    kfab: Kleisli[F, A, B] = kfab,
    succ: Option[Kleisli[F, (A, B), String]] = succ,
    fail: Option[Kleisli[F, (A, Throwable), String]] = fail,
    isWorthRetry: Reader[Throwable, Boolean] = isWorthRetry): NJRetry[F, A, B] =
    new NJRetry[F, A, B](metricRegistry, channel, ongoings, actionParams, kfab, succ, fail, isWorthRetry)

  def withSuccNotesM(succ: (A, B) => F[String]): NJRetry[F, A, B] =
    copy(succ = Some(Kleisli(succ.tupled)))

  def withSuccNotes(f: (A, B) => String): NJRetry[F, A, B] =
    withSuccNotesM((a: A, b: B) => F.pure(f(a, b)))

  def withFailNotesM(fail: (A, Throwable) => F[String]): NJRetry[F, A, B] =
    copy(fail = Some(Kleisli(fail.tupled)))

  def withFailNotes(f: (A, Throwable) => String): NJRetry[F, A, B] =
    withFailNotesM((a: A, b: Throwable) => F.pure(f(a, b)))

  def withWorthRetry(isWorthRetry: Throwable => Boolean): NJRetry[F, A, B] =
    copy(isWorthRetry = Reader(isWorthRetry))

  private[this] lazy val failCounter: Counter = metricRegistry.counter(actionFailMRName(actionParams))
  private[this] lazy val succCounter: Counter = metricRegistry.counter(actionSuccMRName(actionParams))
  private[this] lazy val timer: Timer         = metricRegistry.timer(actionTimerMRName(actionParams))

  private[this] def timingAndCounting(isSucc: Boolean, launchTime: Instant, now: Instant): Unit = {
    if (actionParams.isTiming.value) timer.update(Duration.between(launchTime, now))
    if (actionParams.isCounting.value) {
      if (isSucc) succCounter.inc(1) else failCounter.inc(1)
    }
  }

  private[this] val succNotes: (A, F[B]) => F[Notes] =
    (a: A, b: F[B]) => succ.fold(F.pure(Notes.empty))(k => b.flatMap(k.run(a, _)).map(Notes(_)))

  private[this] val failNotes: (A, Throwable) => F[Notes] =
    (a: A, ex: Throwable) => fail.fold(F.pure(Notes.empty))(_.run((a, ex)).map(Notes(_)))

  private[this] val publisher: ActionEventPublisher[F] = new ActionEventPublisher[F](channel, ongoings)

  def run(input: A): F[B] = for {
    retryCount <- F.ref(0) // hold number of retries
    actionInfo <- publisher.actionStart(actionParams)
    res <- retry.mtl
      .retryingOnSomeErrors[B]
      .apply[F, Throwable](
        actionParams.retry.policy[F],
        isWorthRetry.map(F.pure).run,
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
  fail: Option[Kleisli[F, Throwable, String]],
  isWorthRetry: Reader[Throwable, Boolean]) {
  private def copy(
    metricRegistry: MetricRegistry = metricRegistry,
    channel: Channel[F, NJEvent] = channel,
    ongoings: Ref[F, Set[ActionInfo]] = ongoings,
    actionParams: ActionParams = actionParams,
    fb: F[B] = fb,
    succ: Option[Kleisli[F, B, String]] = succ,
    fail: Option[Kleisli[F, Throwable, String]] = fail,
    isWorthRetry: Reader[Throwable, Boolean] = isWorthRetry): NJRetryUnit[F, B] =
    new NJRetryUnit[F, B](metricRegistry, channel, ongoings, actionParams, fb, succ, fail, isWorthRetry)

  def withSuccNotesM(succ: B => F[String]): NJRetryUnit[F, B] =
    copy(succ = Some(Kleisli(succ)))

  def withSuccNotes(f: B => String): NJRetryUnit[F, B] =
    withSuccNotesM(Kleisli.fromFunction(f).run)

  def withFailNotesM(fail: Throwable => F[String]): NJRetryUnit[F, B] =
    copy(fail = Some(Kleisli(fail)))

  def withFailNotes(f: Throwable => String): NJRetryUnit[F, B] =
    withFailNotesM(Kleisli.fromFunction(f).run)

  def withWorthRetry(isWorthRetry: Throwable => Boolean): NJRetryUnit[F, B] =
    copy(isWorthRetry = Reader(isWorthRetry))

  val run: F[B] =
    new NJRetry[F, Unit, B](
      metricRegistry: MetricRegistry,
      channel: Channel[F, NJEvent],
      ongoings: Ref[F, Set[ActionInfo]],
      actionParams = actionParams,
      kfab = Kleisli(_ => fb),
      succ = succ.map(_.local(_._2)),
      fail = fail.map(_.local(_._2)),
      isWorthRetry = isWorthRetry
    ).run(())
}
