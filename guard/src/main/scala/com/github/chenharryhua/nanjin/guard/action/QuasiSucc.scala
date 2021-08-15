package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.kernel.{Async, Outcome}
import cats.effect.std.UUIDGen
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.{Alternative, Parallel, Traverse}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Importance}
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.{actionStartMRName, actionSuccMRName, realZonedDateTime}
import fs2.concurrent.Channel
import org.apache.commons.lang3.exception.ExceptionUtils

final class QuasiSucc[F[_], T[_], A, B](
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  params: ActionParams,
  input: T[A],
  kfab: Kleisli[F, A, B],
  succ: Kleisli[F, List[(A, B)], String],
  fail: Kleisli[F, List[(A, NJError)], String])(implicit F: Async[F]) {

  def withSuccNotesM(succ: List[(A, B)] => F[String]): QuasiSucc[F, T, A, B] =
    new QuasiSucc[F, T, A, B](
      metricRegistry = metricRegistry,
      channel = channel,
      params = params,
      input = input,
      kfab = kfab,
      succ = Kleisli(succ),
      fail = fail)

  def withSuccNotes(f: List[(A, B)] => String): QuasiSucc[F, T, A, B] =
    withSuccNotesM(Kleisli.fromFunction(f).run)

  def withFailNotesM(fail: List[(A, NJError)] => F[String]): QuasiSucc[F, T, A, B] =
    new QuasiSucc[F, T, A, B](
      metricRegistry = metricRegistry,
      channel = channel,
      params = params,
      input = input,
      kfab = kfab,
      succ = succ,
      fail = Kleisli(fail))

  def withFailNotes(f: List[(A, NJError)] => String): QuasiSucc[F, T, A, B] =
    withFailNotesM(Kleisli.fromFunction(f).run)

  private def internal(eval: F[T[Either[(A, Throwable), (A, B)]]], runMode: RunMode)(implicit
    T: Traverse[T],
    L: Alternative[T]): F[T[B]] = {
    val isFireStartEvent: Boolean = params.importance.value > Importance.Low.value
    val isFireSuccEvent: Boolean  = params.importance.value > Importance.Medium.value
    for {
      now <- realZonedDateTime(params.serviceParams)
      uuid <- UUIDGen.randomUUID
      actionInfo = ActionInfo(id = uuid, launchTime = now)
      _ <-
        if (isFireStartEvent)
          channel.send(
            ActionStart(
              timestamp = now,
              importance = params.importance,
              actionInfo = actionInfo,
              actionParams = params))
        else F.delay(metricRegistry.counter(actionStartMRName(params.actionName)).inc())
      res <- F
        .background(eval.map { fte =>
          val (ex, rs)                   = fte.partitionEither(identity)
          val errors: List[(A, NJError)] = ex.toList.map(e => (e._1, NJError(e._2)))
          (errors, rs) // error on the left, result on the right
        })
        .use(_.flatMap(_.embed(F.raiseError(ActionException.ActionCanceledInternally))))
        .guaranteeCase {
          case Outcome.Canceled() =>
            for {
              now <- realZonedDateTime(params.serviceParams)
              _ <- channel.send(
                ActionFailed(
                  actionInfo = actionInfo,
                  timestamp = now,
                  actionParams = params,
                  numRetries = 0,
                  notes = Notes(ExceptionUtils.getMessage(ActionException.ActionCanceledExternally)),
                  error = NJError(ActionException.ActionCanceledExternally)
                ))
            } yield ()
          case Outcome.Errored(error) =>
            for {
              now <- realZonedDateTime(params.serviceParams)
              _ <- channel.send(
                ActionFailed(
                  actionInfo = actionInfo,
                  timestamp = now,
                  actionParams = params,
                  numRetries = 0,
                  notes = Notes(ExceptionUtils.getMessage(error)),
                  error = NJError(error)
                ))
            } yield ()
          case Outcome.Succeeded(fb) =>
            if (isFireSuccEvent) for {
              now <- realZonedDateTime(params.serviceParams)
              b <- fb
              sn <- succ(b._2.toList)
              fn <- fail(b._1)
              _ <- channel.send(
                ActionQuasiSucced(
                  actionInfo = actionInfo,
                  timestamp = now,
                  importance = params.importance,
                  actionParams = params,
                  runMode = runMode,
                  numSucc = b._2.size,
                  succNotes = Notes(sn),
                  failNotes = Notes(fn),
                  errors = b._1.map(_._2)
                ))
            } yield ()
            else
              F.delay(metricRegistry.counter(actionSuccMRName(params.actionName)).inc())
        }
    } yield T.map(res._2)(_._2)
  }

  def seqRun(implicit T: Traverse[T], L: Alternative[T]): F[T[B]] =
    internal(input.traverse(a => kfab.run(a).attempt.map(_.bimap((a, _), (a, _)))), RunMode.Sequential)

  def parRun(implicit T: Traverse[T], L: Alternative[T], P: Parallel[F]): F[T[B]] =
    internal(input.parTraverse(a => kfab.run(a).attempt.map(_.bimap((a, _), (a, _)))), RunMode.Parallel)

  def parRun(n: Int)(implicit T: Traverse[T], L: Alternative[T]): F[T[B]] =
    internal(F.parTraverseN(n)(input)(a => kfab.run(a).attempt.map(_.bimap((a, _), (a, _)))), RunMode.Parallel)

}

final class QuasiSuccUnit[F[_], T[_], B](
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  params: ActionParams,
  tfb: T[F[B]],
  succ: Kleisli[F, List[B], String],
  fail: Kleisli[F, List[NJError], String])(implicit F: Async[F]) {

  def withSuccNotesM(succ: List[B] => F[String]): QuasiSuccUnit[F, T, B] =
    new QuasiSuccUnit[F, T, B](
      metricRegistry = metricRegistry,
      channel = channel,
      params = params,
      tfb = tfb,
      succ = Kleisli(succ),
      fail = fail)

  def withSuccNotes(f: List[B] => String): QuasiSuccUnit[F, T, B] =
    withSuccNotesM(Kleisli.fromFunction(f).run)

  def withFailNotesM(fail: List[NJError] => F[String]): QuasiSuccUnit[F, T, B] =
    new QuasiSuccUnit[F, T, B](
      metricRegistry = metricRegistry,
      channel = channel,
      params = params,
      tfb = tfb,
      succ = succ,
      fail = Kleisli(fail))

  def withFailNotes(f: List[NJError] => String): QuasiSuccUnit[F, T, B] =
    withFailNotesM(Kleisli.fromFunction(f).run)

  private def toQuasiSucc: QuasiSucc[F, T, F[B], B] =
    new QuasiSucc[F, T, F[B], B](
      metricRegistry = metricRegistry,
      channel = channel,
      params = params,
      input = tfb,
      kfab = Kleisli(identity),
      succ = succ.local(_.map(_._2)),
      fail = fail.local(_.map(_._2)))

  def seqRun(implicit T: Traverse[T], L: Alternative[T]): F[T[B]]                 = toQuasiSucc.seqRun
  def parRun(implicit T: Traverse[T], L: Alternative[T], P: Parallel[F]): F[T[B]] = toQuasiSucc.parRun
  def parRun(n: Int)(implicit T: Traverse[T], L: Alternative[T]): F[T[B]]         = toQuasiSucc.parRun(n)
}
