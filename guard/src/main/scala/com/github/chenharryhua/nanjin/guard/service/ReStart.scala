package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Temporal
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, ServiceStopCause}
import fs2.concurrent.Channel
import fs2.Stream
import org.apache.commons.lang3.exception.ExceptionUtils
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import java.time.{Duration, ZonedDateTime}
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

final private class ReStart[F[_], A](
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  policy: RetryPolicy[F],
  theService: F[A])(implicit F: Temporal[F]) {

  private case class ReStartState(retryStatus: RetryStatus, lastTime: Option[ZonedDateTime])

  private def stop(cause: ServiceStopCause): F[Either[ReStartState, Unit]] =
    publisher.serviceStop(channel, serviceParams, cause).as(Right(()))

  private def panic(
    retryStatus: RetryStatus,
    delay: FiniteDuration,
    err: Throwable): F[Either[ReStartState, Unit]] =
    for {
      ts <- publisher.servicePanic(channel, serviceParams, delay, err)
      _ <- F.sleep(delay)
    } yield Left(ReStartState(retryStatus.addRetry(delay), Some(ts)))

  private def startover(err: Throwable): F[Either[ReStartState, Unit]] =
    policy.decideNextRetry(RetryStatus.NoRetriesYet).flatMap {
      case PolicyDecision.GiveUp => stop(ServiceStopCause.ByGiveup(ExceptionUtils.getMessage(err)))
      case PolicyDecision.DelayAndRetry(delay) => panic(RetryStatus.NoRetriesYet, delay, err)
    }

  private val loop: F[Unit] = F.tailRecM(ReStartState(RetryStatus.NoRetriesYet, None)) { state =>
    (publisher.serviceReStart(channel, serviceParams) >> theService).attempt.flatMap {
      case Right(_) => stop(ServiceStopCause.Normally)
      case Left(err) if !NonFatal(err) =>
        stop(ServiceStopCause.ByException(ExceptionUtils.getRootCauseMessage(err)))
      case Left(err) =>
        policy.decideNextRetry(state.retryStatus).flatMap {
          case PolicyDecision.GiveUp => startover(err)
          // if no error happens for long enough, start over the policies
          case PolicyDecision.DelayAndRetry(delay) =>
            (state.lastTime, serviceParams.policyThreshold)
              .traverseN((last, threshold) =>
                serviceParams.zonedNow.map(now => Duration.between(last, now).compareTo(threshold) > 0))
              .map(_.exists(identity))
              .ifM(startover(err), panic(state.retryStatus, delay, err))
        }
    }
  }

  val stream: Stream[F, Nothing] =
    Stream
      .eval(F.guarantee(F.onCancel(loop, stop(ServiceStopCause.ByCancelation).void), channel.close.void))
      .drain
}
