package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Temporal
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, ServiceStopCause}
import fs2.concurrent.Channel
import fs2.Stream
import org.apache.commons.lang3.exception.ExceptionUtils
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.control.NonFatal

final private class ReStart[F[_], A](
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  policy: RetryPolicy[F],
  fa: F[A])(implicit F: Temporal[F]) {

  private def stopBy(cause: ServiceStopCause): F[Either[RetryStatus, Unit]] =
    publisher.serviceStop(channel, serviceParams, cause) >>
      F.pure[Either[RetryStatus, Unit]](Right(()))

  private def panic(
    retryStatus: RetryStatus,
    delay: FiniteDuration,
    err: Throwable): F[Either[RetryStatus, Unit]] =
    for {
      _ <- publisher.servicePanic(channel, serviceParams, delay, err)
      _ <- F.sleep(delay)
    } yield Left(retryStatus.addRetry(delay))

  private def retrying: F[Unit] = F.tailRecM(RetryStatus.NoRetriesYet) { status =>
    (publisher.serviceReStart(channel, serviceParams) >> fa).attempt.flatMap {
      case Right(_)                    => stopBy(ServiceStopCause.Normally)
      case Left(err) if !NonFatal(err) => stopBy(ServiceStopCause.ByException(ExceptionUtils.getMessage(err)))
      case Left(err) =>
        policy.decideNextRetry(status).flatMap {
          case PolicyDecision.GiveUp => // start over when run out of policies
            policy.decideNextRetry(RetryStatus.NoRetriesYet).flatMap {
              case PolicyDecision.GiveUp => stopBy(ServiceStopCause.ByGiveup(ExceptionUtils.getMessage(err)))
              case PolicyDecision.DelayAndRetry(delay) => panic(RetryStatus.NoRetriesYet, delay, err)
            }
          case PolicyDecision.DelayAndRetry(delay) => panic(status, delay, err)
        }
    }
  }

  def stream: Stream[F, Nothing] =
    Stream
      .eval(
        F.guarantee(
          F.onCancel(retrying, stopBy(ServiceStopCause.ByCancelation).void),
          channel.close >> F.sleep(1.second)))
      .drain
}
