package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Resource.ExitCase
import cats.effect.kernel.Temporal
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.TickStatus
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, ServiceStopCause}
import fs2.Stream
import fs2.concurrent.Channel
import org.apache.commons.lang3.exception.ExceptionUtils
import org.typelevel.cats.time.instances.duration

import java.time.Duration
import scala.jdk.DurationConverters.JavaDurationOps

final private class ReStart[F[_], A](
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  theService: F[A])(implicit F: Temporal[F])
    extends duration {

  private def panic(status: TickStatus, err: Throwable): F[Option[(Unit, TickStatus)]] =
    F.realTimeInstant.flatMap[Option[(Unit, TickStatus)]] { now =>
      val tickStatus: TickStatus = serviceParams.threshold match {
        case Some(threshold) => // if no error occurs long enough, reset the policy
          if (Duration.between(status.tick.acquire, now) > threshold)
            status.renewPolicy(serviceParams.servicePolicies.restart)
          else status
        case None => status
      }

      tickStatus.next(now) match {
        case None =>
          val cause = ServiceStopCause.ByException(ExceptionUtils.getMessage(err))
          publisher.serviceStop(channel, serviceParams, cause).as(None)
        case Some(nts) =>
          for {
            _ <- publisher.servicePanic(channel, serviceParams, nts.tick, err)
            _ <- F.sleep(nts.tick.snooze.toScala)
          } yield Some(((), nts))
      }
    }

  val stream: Stream[F, Nothing] =
    Stream
      .unfoldEval[F, TickStatus, Unit](
        TickStatus(serviceParams.zerothTick).renewPolicy(serviceParams.servicePolicies.restart)) { status =>
        (publisher.serviceReStart(channel, serviceParams, status.tick) <* theService)
          .redeemWith[Option[(Unit, TickStatus)]](
            err => panic(status, err),
            _ => publisher.serviceStop(channel, serviceParams, ServiceStopCause.Normally).as(None)
          )
      }
      .onFinalizeCase {
        case ExitCase.Canceled =>
          publisher.serviceStop(channel, serviceParams, ServiceStopCause.ByCancellation)
        case _ => F.unit
      }
      .drain
}
