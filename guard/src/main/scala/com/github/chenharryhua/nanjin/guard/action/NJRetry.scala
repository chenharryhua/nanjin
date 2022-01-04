package com.github.chenharryhua.nanjin.guard.action

import cats.collections.Predicate
import cats.data.{Kleisli, Reader}
import cats.effect.Temporal
import cats.effect.kernel.Outcome
import cats.effect.std.UUIDGen
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ActionTermination}
import com.github.chenharryhua.nanjin.guard.event.*
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

// https://www.microsoft.com/en-us/research/wp-content/uploads/2016/07/asynch-exns.pdf
final class NJRetry[F[_]: UUIDGen, A, B] private[guard] (
  publisher2: EventPublisher[F],
  params: ActionParams,
  kfab: Kleisli[F, A, B],
  succ: Kleisli[F, (A, B), String],
  fail: Kleisli[F, (A, Throwable), String],
  isWorthRetry: Reader[Throwable, Boolean],
  postCondition: Predicate[B])(implicit F: Temporal[F]) {

  def withSuccNotesM(succ: (A, B) => F[String]): NJRetry[F, A, B] =
    new NJRetry[F, A, B](
      publisher2 = publisher2,
      params = params,
      kfab = kfab,
      succ = Kleisli(succ.tupled),
      fail = fail,
      isWorthRetry = isWorthRetry,
      postCondition = postCondition)

  def withSuccNotes(f: (A, B) => String): NJRetry[F, A, B] =
    withSuccNotesM((a: A, b: B) => F.pure(f(a, b)))

  def withFailNotesM(fail: (A, Throwable) => F[String]): NJRetry[F, A, B] =
    new NJRetry[F, A, B](
      publisher2 = publisher2,
      params = params,
      kfab = kfab,
      succ = succ,
      fail = Kleisli(fail.tupled),
      isWorthRetry = isWorthRetry,
      postCondition = postCondition)

  def withFailNotes(f: (A, Throwable) => String): NJRetry[F, A, B] =
    withFailNotesM((a: A, b: Throwable) => F.pure(f(a, b)))

  def withWorthRetry(isWorthRetry: Throwable => Boolean): NJRetry[F, A, B] =
    new NJRetry[F, A, B](
      publisher2 = publisher2,
      params = params,
      kfab = kfab,
      succ = succ,
      fail = fail,
      isWorthRetry = Reader(isWorthRetry),
      postCondition = postCondition)

  def withPostCondition(postCondition: B => Boolean): NJRetry[F, A, B] =
    new NJRetry[F, A, B](
      publisher2 = publisher2,
      params = params,
      kfab = kfab,
      succ = succ,
      fail = fail,
      isWorthRetry = isWorthRetry,
      postCondition = Predicate(postCondition))

  def run(input: A): F[B] = for {
    retryCount <- F.ref(0) // hold number of retries
    ts <- realZonedDateTime2(params)
    uuid <- UUIDGen.randomUUID[F]
    publisher = new ActionEventPublisher[F](
      ActionInfo(params, uuid, ts),
      publisher2.metricRegistry,
      publisher2.channel,
      publisher2.ongoings)
    _ <- publisher.actionStart
    res <- F.uncancelable(poll =>
      retry.mtl
        .retryingOnSomeErrors[B]
        .apply[F, Throwable](
          params.retry.policy[F],
          isWorthRetry.map(F.pure).run,
          (error, details) =>
            details match {
              case wdr: WillDelayAndRetry => publisher.actionRetry(retryCount, wdr, error)
              case _: GivingUp            => F.unit
            }
        ) {
          for {
            gate <- F.deferred[Outcome[F, Throwable, B]]
            fiber <- F.start(kfab.run(input).guaranteeCase(gate.complete(_).void))
            oc <- F.onCancel(
              poll(gate.get).flatMap(_.embed(F.raiseError[B](ActionException.ActionCanceledInternally))),
              fiber.cancel)
            _ <- F.raiseError(ActionException.UnexpectedlyTerminated).whenA(params.isTerminate === ActionTermination.No)
            _ <- succ(input, oc)
              .flatMap[B](msg => F.raiseError(ActionException.PostConditionUnsatisfied(msg)))
              .whenA(!postCondition(oc))
          } yield oc
        }
        .guaranteeCase {
          case Outcome.Canceled() =>
            val error = ActionException.ActionCanceledExternally
            publisher.actionFail[A](retryCount, input, error, fail)
          case Outcome.Errored(error)    => publisher.actionFail[A](retryCount, input, error, fail)
          case Outcome.Succeeded(output) => publisher.actionSucc[A, B](retryCount, input, output, succ)
        })
  } yield res
}

final class NJRetryUnit[F[_]: Temporal: UUIDGen, B] private[guard] (
  fb: F[B],
  publisher: EventPublisher[F],
  params: ActionParams,
  succ: Kleisli[F, B, String],
  fail: Kleisli[F, Throwable, String],
  isWorthRetry: Reader[Throwable, Boolean],
  postCondition: Predicate[B]) {

  def withSuccNotesM(succ: B => F[String]): NJRetryUnit[F, B] =
    new NJRetryUnit[F, B](
      fb = fb,
      publisher = publisher,
      params = params,
      succ = Kleisli(succ),
      fail = fail,
      isWorthRetry = isWorthRetry,
      postCondition = postCondition)

  def withSuccNotes(f: B => String): NJRetryUnit[F, B] =
    withSuccNotesM(Kleisli.fromFunction(f).run)

  def withFailNotesM(fail: Throwable => F[String]): NJRetryUnit[F, B] =
    new NJRetryUnit[F, B](
      fb = fb,
      publisher = publisher,
      params = params,
      succ = succ,
      fail = Kleisli(fail),
      isWorthRetry = isWorthRetry,
      postCondition = postCondition)

  def withFailNotes(f: Throwable => String): NJRetryUnit[F, B] =
    withFailNotesM(Kleisli.fromFunction(f).run)

  def withWorthRetry(isWorthRetry: Throwable => Boolean): NJRetryUnit[F, B] =
    new NJRetryUnit[F, B](
      fb = fb,
      publisher = publisher,
      params = params,
      succ = succ,
      fail = fail,
      isWorthRetry = Reader(isWorthRetry),
      postCondition = postCondition)

  def withPostCondition(postCondition: B => Boolean): NJRetryUnit[F, B] =
    new NJRetryUnit[F, B](
      fb = fb,
      publisher = publisher,
      params = params,
      succ = succ,
      fail = fail,
      isWorthRetry = isWorthRetry,
      postCondition = Predicate(postCondition))

  val run: F[B] =
    new NJRetry[F, Unit, B](
      publisher2 = publisher,
      params = params,
      kfab = Kleisli(_ => fb),
      succ = succ.local(_._2),
      fail = fail.local(_._2),
      isWorthRetry = isWorthRetry,
      postCondition = postCondition
    ).run(())
}
