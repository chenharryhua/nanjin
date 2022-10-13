package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Temporal
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError, NJEvent}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ActionRetry
import fs2.concurrent.Channel
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.control.NonFatal

final private class ReTry[F[_], OUT](
  channel: Channel[F, NJEvent],
  policy: RetryPolicy[F],
  isWorthRetry: Throwable => F[Boolean],
  action: F[OUT],
  actionInfo: ActionInfo
)(implicit F: Temporal[F]) {

  @inline private[this] def fail(ex: Throwable): F[Either[RetryStatus, OUT]] =
    F.raiseError[OUT](ex).map(Right(_))

  private[this] def retrying(ex: Throwable, status: RetryStatus): F[Either[RetryStatus, OUT]] =
    policy.decideNextRetry(status).flatMap {
      case PolicyDecision.GiveUp => fail(ex)
      case PolicyDecision.DelayAndRetry(delay) =>
        for {
          ts <- actionInfo.actionParams.serviceParams.zonedNow
          _ <- channel
            .send(
              ActionRetry(
                actionInfo = actionInfo,
                timestamp = ts,
                retriesSoFar = status.retriesSoFar,
                nextRetryTime = ts.plus(delay.toJava),
                error = NJError(ex)
              ))
            .whenA(actionInfo.actionParams.isNonTrivial)
          _ <- F.sleep(delay)
        } yield Left(status.addRetry(delay))
    }

  val execute: F[OUT] = F.tailRecM(RetryStatus.NoRetriesYet) { status =>
    action.attempt.flatMap {
      case Right(out)                => F.pure[Either[RetryStatus, OUT]](Right(out))
      case Left(ex) if !NonFatal(ex) => fail(ex)
      case Left(ex)                  => isWorthRetry(ex).ifM(retrying(ex, status), fail(ex))
    }
  }
}
