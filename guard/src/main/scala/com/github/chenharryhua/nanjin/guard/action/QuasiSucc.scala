package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, Reader}
import cats.effect.kernel.{Async, Outcome, Ref}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.{Alternative, Parallel, Traverse}
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionFailed,
  ActionInfo,
  ActionQuasiSucced,
  ActionStart,
  DailySummaries,
  NJError,
  NJEvent,
  Notes,
  RunMode,
  ServiceInfo
}
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.realZonedDateTime
import fs2.concurrent.Channel
import org.apache.commons.lang3.exception.ExceptionUtils

import java.util.UUID

final class QuasiSucc[F[_], T[_], A, B](
  serviceInfo: ServiceInfo,
  dailySummaries: Ref[F, DailySummaries],
  channel: Channel[F, NJEvent],
  actionName: String,
  params: ActionParams,
  input: T[A],
  kfab: Kleisli[F, A, B],
  succ: Reader[List[(A, B)], String],
  fail: Reader[List[(A, NJError)], String]) {

  def withSuccNotes(succ: List[(A, B)] => String): QuasiSucc[F, T, A, B] =
    new QuasiSucc[F, T, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      kfab = kfab,
      succ = Reader(succ),
      fail = fail)

  def withFailNotes(fail: List[(A, NJError)] => String): QuasiSucc[F, T, A, B] =
    new QuasiSucc[F, T, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      kfab = kfab,
      succ = succ,
      fail = Reader(fail))

  private def internal(eval: F[T[Either[(A, Throwable), (A, B)]]], runMode: RunMode)(implicit
    F: Async[F],
    T: Traverse[T],
    L: Alternative[T]): F[T[B]] =
    for {
      now <- realZonedDateTime(params.serviceParams)
      actionInfo = ActionInfo(
        actionName = actionName,
        serviceInfo = serviceInfo,
        id = UUID.randomUUID(),
        launchTime = now)
      _ <- channel.send(ActionStart(now, actionInfo, params))
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
              _ <- dailySummaries.update(_.incActionFail)
              _ <- channel.send(
                ActionFailed(
                  timestamp = now,
                  actionInfo = actionInfo,
                  actionParams = params,
                  numRetries = 0,
                  notes = Notes(ExceptionUtils.getMessage(ActionException.ActionCanceledExternally)),
                  error = NJError(ActionException.ActionCanceledExternally)
                ))
            } yield ()
          case Outcome.Errored(error) =>
            for {
              now <- realZonedDateTime(params.serviceParams)
              _ <- dailySummaries.update(_.incActionFail)
              _ <- channel.send(
                ActionFailed(
                  timestamp = now,
                  actionInfo = actionInfo,
                  actionParams = params,
                  numRetries = 0,
                  notes = Notes(ExceptionUtils.getMessage(error)),
                  error = NJError(error)
                ))
            } yield ()
          case Outcome.Succeeded(fb) =>
            for {
              now <- realZonedDateTime(params.serviceParams)
              b <- fb
              _ <- dailySummaries.update(_.incActionSucc)
              _ <- channel.send(
                ActionQuasiSucced(
                  timestamp = now,
                  actionInfo = actionInfo,
                  actionParams = params,
                  runMode = runMode,
                  numSucc = b._2.size,
                  succNotes = Notes(succ(b._2.toList)),
                  failNotes = Notes(fail(b._1)),
                  errors = b._1.map(_._2)
                ))
            } yield ()
        }
    } yield T.map(res._2)(_._2)

  def seqRun(implicit F: Async[F], T: Traverse[T], L: Alternative[T]): F[T[B]] =
    internal(input.traverse(a => kfab.run(a).attempt.map(_.bimap((a, _), (a, _)))), RunMode.Sequential)

  def parRun(implicit F: Async[F], T: Traverse[T], L: Alternative[T], P: Parallel[F]): F[T[B]] =
    internal(input.parTraverse(a => kfab.run(a).attempt.map(_.bimap((a, _), (a, _)))), RunMode.Parallel)

  def parRun(n: Int)(implicit F: Async[F], T: Traverse[T], L: Alternative[T], P: Parallel[F]): F[T[B]] =
    internal(F.parTraverseN(n)(input)(a => kfab.run(a).attempt.map(_.bimap((a, _), (a, _)))), RunMode.Parallel)

}

final class QuasiSuccUnit[F[_], T[_], B](
  serviceInfo: ServiceInfo,
  dailySummaries: Ref[F, DailySummaries],
  channel: Channel[F, NJEvent],
  actionName: String,
  params: ActionParams,
  tfb: T[F[B]],
  succ: Reader[List[B], String],
  fail: Reader[List[NJError], String]) {

  def withSuccNotes(succ: List[B] => String): QuasiSuccUnit[F, T, B] =
    new QuasiSuccUnit[F, T, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      tfb = tfb,
      succ = Reader(succ),
      fail = fail)

  def withFailNotes(fail: List[NJError] => String): QuasiSuccUnit[F, T, B] =
    new QuasiSuccUnit[F, T, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      tfb = tfb,
      succ = succ,
      fail = Reader(fail))

  private def toQuasiSucc: QuasiSucc[F, T, F[B], B] =
    new QuasiSucc[F, T, F[B], B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = tfb,
      kfab = Kleisli(identity),
      succ = succ.local(_.map(_._2)),
      fail = fail.local(_.map(_._2)))

  def seqRun(implicit F: Async[F], T: Traverse[T], L: Alternative[T]): F[T[B]] =
    toQuasiSucc.seqRun

  def parRun(implicit F: Async[F], T: Traverse[T], L: Alternative[T], P: Parallel[F]): F[T[B]] =
    toQuasiSucc.parRun

  def parRun(n: Int)(implicit F: Async[F], T: Traverse[T], L: Alternative[T], P: Parallel[F]): F[T[B]] =
    toQuasiSucc.parRun(n)
}
