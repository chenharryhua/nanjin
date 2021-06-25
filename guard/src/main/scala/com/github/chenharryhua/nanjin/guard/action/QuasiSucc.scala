package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, Reader}
import cats.effect.syntax.all._
import cats.effect.{Async, Outcome, Ref}
import cats.syntax.all._
import cats.{Alternative, Parallel, Traverse}
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionFailed,
  ActionInfo,
  ActionQuasiSucced,
  DailySummaries,
  NJError,
  NJEvent,
  Notes,
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
  fab: Kleisli[F, A, B],
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
      fab = fab,
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
      fab = fab,
      succ = succ,
      fail = Reader(fail))

  private def internal(
    eval: F[T[Either[(A, Throwable), (A, B)]]])(implicit F: Async[F], T: Traverse[T], L: Alternative[T]): F[T[B]] =
    for {
      now <- realZonedDateTime(params.serviceParams)
      actionInfo = ActionInfo(
        actionName = actionName,
        serviceInfo = serviceInfo,
        id = UUID.randomUUID(),
        launchTime = now)
      res <- F
        .background(eval.map { fte =>
          val (ex, rs)                   = fte.partitionEither(identity)
          val errors: List[(A, NJError)] = ex.toList.map(e => (e._1, NJError(e._2)))
          (errors, rs) // error on the left, result on the right
        })
        .use(_.flatMap(_.embed(F.raiseError(ActionCanceledInternally(actionName)))))
        .guaranteeCase {
          case Outcome.Canceled() =>
            val error = ActionCanceledExternally(actionName)
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
              _ <- dailySummaries.update(d =>
                d.copy(actionSucc = d.actionSucc + b._2.size, actionFail = d.actionFail + b._1.size))
              _ <- channel.send(
                ActionQuasiSucced(
                  timestamp = now,
                  actionInfo = actionInfo,
                  actionParams = params,
                  numSucc = b._2.size,
                  succNotes = Notes(succ(b._2.toList)),
                  failNotes = Notes(fail(b._1)),
                  errors = b._1.map(_._2)
                ))
            } yield ()
        }
    } yield T.map(res._2)(_._2)

  def seqRun(implicit F: Async[F], T: Traverse[T], L: Alternative[T]): F[T[B]] =
    internal(input.traverse(a => fab.run(a).attempt.map(_.bimap((a, _), (a, _)))))

  def parRun(implicit F: Async[F], T: Traverse[T], L: Alternative[T], P: Parallel[F]): F[T[B]] =
    internal(input.parTraverse(a => fab.run(a).attempt.map(_.bimap((a, _), (a, _)))))

}
