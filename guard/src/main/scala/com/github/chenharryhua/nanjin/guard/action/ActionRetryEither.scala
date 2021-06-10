package com.github.chenharryhua.nanjin.guard.action

import cats.data.{EitherT, Kleisli, Reader}
import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionFailed,
  ActionInfo,
  ActionSucced,
  DailySummaries,
  NJError,
  NJEvent,
  ServiceInfo
}
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import fs2.concurrent.Channel
import retry.RetryDetails.GivingUp
import retry.RetryPolicies

import java.util.UUID
import scala.concurrent.duration.Duration

/** When outer F[_] fails, return immedidately only retry when the inner Either is on the left branch
  */
final class ActionRetryEither[F[_], A, B](
  serviceInfo: ServiceInfo,
  dailySummaries: Ref[F, DailySummaries],
  channel: Channel[F, NJEvent],
  actionName: String,
  params: ActionParams,
  input: A,
  eitherT: EitherT[Kleisli[F, A, *], Throwable, B],
  succ: Reader[(A, B), String],
  fail: Reader[(A, Throwable), String]) {

  def withSuccNotes(succ: (A, B) => String): ActionRetryEither[F, A, B] =
    new ActionRetryEither[F, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      eitherT = eitherT,
      succ = Reader(succ.tupled),
      fail = fail)

  def withFailNotes(fail: (A, Throwable) => String): ActionRetryEither[F, A, B] =
    new ActionRetryEither[F, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      eitherT = eitherT,
      succ = succ,
      fail = Reader(fail.tupled))

  def run(implicit F: Async[F]): F[B] = Ref.of[F, Int](0).flatMap(internalRun)

  private def internalRun(ref: Ref[F, Int])(implicit F: Async[F]): F[B] =
    F.realTimeInstant.map(_.atZone(params.serviceParams.taskParams.zoneId)).flatMap { ts =>
      val actionInfo: ActionInfo =
        ActionInfo(actionName = actionName, serviceInfo = serviceInfo, id = UUID.randomUUID(), launchTime = ts)

      val base = new ActionRetryBase[F, A, B](input, succ, fail)

      retry
        .retryingOnAllErrors[Either[Throwable, B]](
          params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
          base.onError(actionInfo, params, channel, ref, dailySummaries)) {
          eitherT.value.run(input).attempt.flatMap {
            case Left(error) =>
              for {
                now <- F.realTimeInstant.map(_.atZone(params.serviceParams.taskParams.zoneId))
                _ <- channel.send(
                  ActionFailed(
                    timestamp = now,
                    actionInfo = actionInfo,
                    params = params,
                    givingUp = GivingUp(0, Duration.Zero),
                    notes = base.failNotes(error),
                    error = NJError(error)))
                _ <- dailySummaries.update(_.incActionFail)
              } yield Left(error)
            case Right(outerRight) =>
              outerRight match {
                case Left(ex)     => F.raiseError(ex)
                case r @ Right(_) => F.pure(r)
              }
          }
        }
        .rethrow
        .flatTap(b =>
          for {
            count <- ref.get
            now <- F.realTimeInstant.map(_.atZone(params.serviceParams.taskParams.zoneId))
            _ <- channel.send(
              ActionSucced(
                timestamp = now,
                actionInfo = actionInfo,
                params = params,
                numRetries = count,
                notes = base.succNotes(b)))
            _ <- dailySummaries.update(_.incActionSucc)
          } yield ())
    }
}
