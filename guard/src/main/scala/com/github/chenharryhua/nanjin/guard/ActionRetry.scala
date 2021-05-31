package com.github.chenharryhua.nanjin.guard

import cats.data.{Kleisli, Reader}
import cats.effect.{Async, Ref}
import cats.syntax.all._
import fs2.concurrent.Topic
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies}

import java.time.{Duration => JavaDuration}
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

final class ActionRetry[F[_], A, B](
  topic: Topic[F, NJEvent],
  serviceInfo: ServiceInfo,
  actionName: String,
  config: ActionConfig,
  input: A,
  kleisli: Kleisli[F, A, B],
  succ: Reader[(A, B), String],
  fail: Reader[(A, Throwable), String]
) {
  val params: ActionParams = config.evalConfig

  def withSuccInfo(succ: (A, B) => String): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](topic, serviceInfo, actionName, config.succOn, input, kleisli, Reader(succ.tupled), fail)

  def withFailInfo(fail: (A, Throwable) => String): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](topic, serviceInfo, actionName, config.failOn, input, kleisli, succ, Reader(fail.tupled))

  def run(implicit F: Async[F]): F[B] = Ref.of[F, Int](0).flatMap(ref => internalRun(ref))

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

    retry
      .retryingOnAllErrors[B](
        params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
        onError)(kleisli.run(input))
      .flatTap(b =>
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
