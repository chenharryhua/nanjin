package com.github.chenharryhua.nanjin.guard.action

import cats.collections.Predicate
import cats.data.{Kleisli, Reader}
import cats.effect.Temporal
import cats.effect.kernel.{Outcome, Ref}
import cats.effect.std.UUIDGen
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ActionTermination, CountAction, TimeAction}
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
  succ: Kleisli[F, (A, B), String],
  fail: Kleisli[F, (A, Throwable), String],
  isWorthRetry: Reader[Throwable, Boolean],
  postCondition: Predicate[B])(implicit F: Temporal[F]) {
  private def copy(
    metricRegistry: MetricRegistry = metricRegistry,
    channel: Channel[F, NJEvent] = channel,
    ongoings: Ref[F, Set[ActionInfo]] = ongoings,
    actionParams: ActionParams = actionParams,
    kfab: Kleisli[F, A, B] = kfab,
    succ: Kleisli[F, (A, B), String] = succ,
    fail: Kleisli[F, (A, Throwable), String] = fail,
    isWorthRetry: Reader[Throwable, Boolean] = isWorthRetry,
    postCondition: Predicate[B] = postCondition): NJRetry[F, A, B] =
    new NJRetry[F, A, B](metricRegistry, channel, ongoings, actionParams, kfab, succ, fail, isWorthRetry, postCondition)

  def withSuccNotesM(succ: (A, B) => F[String]): NJRetry[F, A, B] =
    copy(succ = Kleisli(succ.tupled))

  def withSuccNotes(f: (A, B) => String): NJRetry[F, A, B] =
    withSuccNotesM((a: A, b: B) => F.pure(f(a, b)))

  def withFailNotesM(fail: (A, Throwable) => F[String]): NJRetry[F, A, B] =
    copy(fail = Kleisli(fail.tupled))

  def withFailNotes(f: (A, Throwable) => String): NJRetry[F, A, B] =
    withFailNotesM((a: A, b: Throwable) => F.pure(f(a, b)))

  def withWorthRetry(isWorthRetry: Throwable => Boolean): NJRetry[F, A, B] =
    copy(isWorthRetry = Reader(isWorthRetry))

  def withPostCondition(postCondition: B => Boolean): NJRetry[F, A, B] =
    copy(postCondition = Predicate(postCondition))

  private lazy val failCounter: Counter = metricRegistry.counter(actionFailMRName(actionParams))
  private lazy val succCounter: Counter = metricRegistry.counter(actionSuccMRName(actionParams))
  private lazy val timer: Timer         = metricRegistry.timer(actionTimerMRName(actionParams))

  private def timingAndCounting(isSucc: Boolean, launchTime: Instant, now: Instant): Unit = {
    if (actionParams.isTiming === TimeAction.Yes) timer.update(Duration.between(launchTime, now))
    if (actionParams.isCounting === CountAction.Yes) {
      if (isSucc) succCounter.inc(1) else failCounter.inc(1)
    }
  }

  private val publisher: ActionEventPublisher[F] = new ActionEventPublisher[F](channel, ongoings)

  def run(input: A): F[B] = for {
    retryCount <- F.ref(0) // hold number of retries
    actionInfo <- publisher.actionStart(actionParams)
    res <- F.uncancelable(poll =>
      retry.mtl
        .retryingOnSomeErrors[B]
        .apply[F, Throwable](
          actionParams.retry.policy[F],
          isWorthRetry.map(F.pure).run,
          (error, details) =>
            details match {
              case wdr: WillDelayAndRetry => publisher.actionRetry(actionInfo, retryCount, wdr, error)
              case _: GivingUp            => F.unit
            }
        ) {
          for {
            gate <- F.deferred[Outcome[F, Throwable, B]]
            fiber <- F.start(kfab.run(input).guaranteeCase(gate.complete(_).void))
            oc <- F.onCancel(
              poll(gate.get).flatMap(_.embed(F.raiseError[B](ActionException.ActionCanceledInternally))),
              fiber.cancel)
            _ <- F
              .raiseError(ActionException.UnexpectedlyTerminated)
              .whenA(actionParams.isTerminate === ActionTermination.No)
            _ <- succ(input, oc)
              .flatMap[B](msg => F.raiseError(ActionException.PostConditionUnsatisfied(msg)))
              .whenA(!postCondition(oc))
          } yield oc
        }
        .guaranteeCase {
          case Outcome.Canceled() =>
            val error = ActionException.ActionCanceledExternally
            publisher
              .actionFail[A](actionInfo, retryCount, input, error, fail)
              .map(ts => timingAndCounting(isSucc = false, actionInfo.launchTime, ts))
          case Outcome.Errored(error) =>
            publisher
              .actionFail[A](actionInfo, retryCount, input, error, fail)
              .map(ts => timingAndCounting(isSucc = false, actionInfo.launchTime, ts))
          case Outcome.Succeeded(output) =>
            publisher
              .actionSucc[A, B](actionInfo, retryCount, input, output, succ)
              .map(ts => timingAndCounting(isSucc = true, actionInfo.launchTime, ts))
        })
  } yield res
}

final class NJRetryUnit[F[_]: Temporal: UUIDGen, B] private[guard] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  ongoings: Ref[F, Set[ActionInfo]],
  actionParams: ActionParams,
  fb: F[B],
  succ: Kleisli[F, B, String],
  fail: Kleisli[F, Throwable, String],
  isWorthRetry: Reader[Throwable, Boolean],
  postCondition: Predicate[B]) {
  private def copy(
    metricRegistry: MetricRegistry = metricRegistry,
    channel: Channel[F, NJEvent] = channel,
    ongoings: Ref[F, Set[ActionInfo]] = ongoings,
    actionParams: ActionParams = actionParams,
    fb: F[B] = fb,
    succ: Kleisli[F, B, String] = succ,
    fail: Kleisli[F, Throwable, String] = fail,
    isWorthRetry: Reader[Throwable, Boolean] = isWorthRetry,
    postCondition: Predicate[B] = postCondition): NJRetryUnit[F, B] =
    new NJRetryUnit[F, B](metricRegistry, channel, ongoings, actionParams, fb, succ, fail, isWorthRetry, postCondition)

  def withSuccNotesM(succ: B => F[String]): NJRetryUnit[F, B] =
    copy(succ = Kleisli(succ))

  def withSuccNotes(f: B => String): NJRetryUnit[F, B] =
    withSuccNotesM(Kleisli.fromFunction(f).run)

  def withFailNotesM(fail: Throwable => F[String]): NJRetryUnit[F, B] =
    copy(fail = Kleisli(fail))

  def withFailNotes(f: Throwable => String): NJRetryUnit[F, B] =
    withFailNotesM(Kleisli.fromFunction(f).run)

  def withWorthRetry(isWorthRetry: Throwable => Boolean): NJRetryUnit[F, B] =
    copy(isWorthRetry = Reader(isWorthRetry))

  def withPostCondition(postCondition: B => Boolean): NJRetryUnit[F, B] =
    copy(postCondition = Predicate(postCondition))

  val run: F[B] =
    new NJRetry[F, Unit, B](
      metricRegistry: MetricRegistry,
      channel: Channel[F, NJEvent],
      ongoings: Ref[F, Set[ActionInfo]],
      actionParams = actionParams,
      kfab = Kleisli(_ => fb),
      succ = succ.local(_._2),
      fail = fail.local(_._2),
      isWorthRetry = isWorthRetry,
      postCondition = postCondition
    ).run(())
}
