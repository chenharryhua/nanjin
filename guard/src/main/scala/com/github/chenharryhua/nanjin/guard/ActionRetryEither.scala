package com.github.chenharryhua.nanjin.guard

import cats.data.{EitherT, Kleisli, Reader}
import cats.effect.{Async, Ref}
import cats.syntax.all._
import fs2.concurrent.Topic
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies}

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import java.time.{Duration => JavaDuration}

/** When outer F[_] fails, return immedidately
  * only retry when the inner Either is on the left branch
  */
final class ActionRetryEither[F[_], A, B](
  topic: Topic[F, NJEvent],
  serviceInfo: ServiceInfo,
  actionName: String,
  config: ActionConfig,
  input: A,
  eitherT: EitherT[Kleisli[F, A, *], Throwable, B],
  succ: Reader[(A, B), String],
  fail: Reader[(A, Throwable), String]
) {
  val params: ActionParams = config.evalConfig

  def run(implicit F: Async[F]): F[B] = Ref.of[F, Int](0).flatMap(ref => internalRun(ref))

  def withSuccInfo(succ: (A, B) => String): ActionRetryEither[F, A, B] =
    new ActionRetryEither[F, A, B](
      topic,
      serviceInfo,
      actionName,
      config.succOn,
      input,
      eitherT,
      Reader(succ.tupled),
      fail)

  def withFailInfo(fail: (A, Throwable) => String): ActionRetryEither[F, A, B] =
    new ActionRetryEither[F, A, B](
      topic,
      serviceInfo,
      actionName,
      config.failOn,
      input,
      eitherT,
      succ,
      Reader(fail.tupled))

  private def internalRun(ref: Ref[F, Int])(implicit F: Async[F]): F[B] = F.realTimeInstant.flatMap { ts =>
    val actionInfo: ActionInfo =
      ActionInfo(
        applicationName = serviceInfo.applicationName,
        serviceName = serviceInfo.serviceName,
        actionName = actionName,
        params = params,
        id = UUID.randomUUID(),
        launchTime = ts
      )

    def onError(error: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case wdr @ WillDelayAndRetry(_, _, _) =>
          topic
            .publish1(
              ActionRetrying(
                actionInfo = actionInfo,
                willDelayAndRetry = wdr,
                error = error
              ))
            .void <* ref.update(_ + 1)
        case gu @ GivingUp(_, _) =>
          for {
            now <- F.realTimeInstant
            _ <- topic.publish1(
              ActionFailed(
                actionInfo = actionInfo,
                givingUp = gu,
                duration = FiniteDuration(JavaDuration.between(ts, now).toNanos, TimeUnit.NANOSECONDS),
                notes = fail.run((input, error)),
                error = error
              ))
          } yield ()
      }

    val res: F[Either[Throwable, B]] = retry.retryingOnAllErrors[Either[Throwable, B]](
      params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
      onError) {
      eitherT.value.run(input).attempt.flatMap {
        case Left(ex) => F.pure(Left(ex))
        case Right(outerRight) =>
          outerRight match {
            case Left(ex)          => F.raiseError(ex)
            case Right(innerRight) => F.pure(Right(innerRight))
          }
      }
    }
    res.rethrow.flatTap(b =>
      for {
        count <- ref.get
        now <- F.realTimeInstant
        _ <- topic.publish1(
          ActionSucced(
            actionInfo = actionInfo,
            duration = FiniteDuration(JavaDuration.between(ts, now).toNanos, TimeUnit.NANOSECONDS),
            numRetries = count,
            notes = succ.run((input, b))))
      } yield ())
  }
}
