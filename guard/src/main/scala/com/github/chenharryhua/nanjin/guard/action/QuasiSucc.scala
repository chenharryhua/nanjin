package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, Reader}
import cats.effect.{Async, Ref}
import cats.syntax.all._
import cats.{Alternative, Functor, Traverse}
import com.github.chenharryhua.nanjin.guard.alert.{
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
  fail: Reader[List[(A, Throwable)], String]) {

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

  def withFailNotes(fail: List[(A, Throwable)] => String): QuasiSucc[F, T, A, B] =
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

  def run(implicit F: Async[F], T: Traverse[T], L: Alternative[T]): F[T[B]] =
    for {
      now <- realZonedDateTime(params.serviceParams)
      actionInfo = ActionInfo(
        actionName = actionName,
        serviceInfo = serviceInfo,
        id = UUID.randomUUID(),
        launchTime = now)
      res <- input.traverse(a => fab.run(a).attempt.map(_.bimap((a, _), (a, _)))).flatMap { r =>
        val (ex, rs) = r.partitionEither(identity)
        for {
          ts <- realZonedDateTime(params.serviceParams)
          rs <- channel
            .send(ActionQuasiSucced(
              timestamp = ts,
              actionInfo = actionInfo,
              params = params,
              numSucc = rs.size,
              notes = Notes(if (ex.isEmpty) succ(rs.toList) else fail(ex.toList)),
              errors = ex.toList.map(e => NJError(e._2))
            ))
            .as(rs)
          _ <- dailySummaries.update(d =>
            d.copy(actionSucc = d.actionSucc + rs.size, actionFail = d.actionFail + ex.size))
        } yield rs
      }
    } yield T.map(res)(_._2)
}
