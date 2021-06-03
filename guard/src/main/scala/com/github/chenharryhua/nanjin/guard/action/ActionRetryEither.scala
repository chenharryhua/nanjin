package com.github.chenharryhua.nanjin.guard.action

import cats.data.{EitherT, Kleisli, Reader}
import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.alert.{ActionFailed, ActionInfo, ActionSucced, NJEvent}
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ActionParams}
import fs2.concurrent.Topic
import retry.RetryDetails.GivingUp
import retry.RetryPolicies

import java.util.UUID
import scala.concurrent.duration.Duration

/** When outer F[_] fails, return immedidately only retry when the inner Either is on the left branch
  */
final class ActionRetryEither[F[_], A, B](
  topic: Topic[F, NJEvent],
  applicationName: String,
  parentName: String,
  actionName: String,
  config: ActionConfig,
  input: A,
  eitherT: EitherT[Kleisli[F, A, *], Throwable, B],
  succ: Reader[(A, B), String],
  fail: Reader[(A, Throwable), String]) {
  val params: ActionParams = config.evalConfig

  def run(implicit F: Async[F]): F[B] = Ref.of[F, Int](0).flatMap(internalRun)

  def withSuccNotes(succ: (A, B) => String): ActionRetryEither[F, A, B] =
    new ActionRetryEither[F, A, B](
      topic,
      applicationName,
      parentName,
      actionName,
      config,
      input,
      eitherT,
      Reader(succ.tupled),
      fail)

  def withFailNotes(fail: (A, Throwable) => String): ActionRetryEither[F, A, B] =
    new ActionRetryEither[F, A, B](
      topic,
      applicationName,
      parentName,
      actionName,
      config,
      input,
      eitherT,
      succ,
      Reader(fail.tupled))

  private def internalRun(ref: Ref[F, Int])(implicit F: Async[F]): F[B] = F.realTimeInstant.flatMap { ts =>
    val actionInfo: ActionInfo =
      ActionInfo(
        applicationName = applicationName,
        parentName = parentName,
        actionName = actionName,
        params = params,
        id = UUID.randomUUID(),
        launchTime = ts
      )

    val base = new ActionRetryBase[F, A, B](input, succ, fail)

    retry
      .retryingOnAllErrors[Either[Throwable, B]](
        params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
        base.onError(actionInfo, topic, ref)) {
        eitherT.value.run(input).attempt.flatMap {
          case Left(error) =>
            for {
              now <- F.realTimeInstant
              _ <- topic.publish1(
                ActionFailed(
                  actionInfo = actionInfo,
                  givingUp = GivingUp(0, Duration.Zero),
                  endAt = now,
                  notes = base.failNotes(error),
                  error = error))
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
          now <- F.realTimeInstant
          _ <- topic.publish1(ActionSucced(actionInfo, now, count, base.succNotes(b)))
        } yield ())
  }
}
