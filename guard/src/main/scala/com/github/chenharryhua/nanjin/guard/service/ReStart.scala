package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Resource.ExitCase
import cats.effect.kernel.Temporal
import cats.effect.std.AtomicCell
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.TickStatus
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.ServicePanic
import com.github.chenharryhua.nanjin.guard.event.{Error, Event, ServiceStopCause}
import fs2.Stream
import fs2.concurrent.Channel
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.typelevel.cats.time.instances.duration

import java.time.Duration
import scala.jdk.DurationConverters.JavaDurationOps

final private class ReStart[F[_]: Temporal](
  channel: Channel[F, Event],
  serviceParams: ServiceParams,
  panicHistory: AtomicCell[F, CircularFifoQueue[ServicePanic]],
  theService: F[Unit])
    extends duration {
  private[this] val F = Temporal[F]

  private def panic(status: TickStatus, ex: Throwable): F[Option[(Unit, TickStatus)]] =
    F.realTimeInstant.flatMap[Option[(Unit, TickStatus)]] { now =>
      val tickStatus: TickStatus = serviceParams.threshold match {
        case Some(threshold) =>
          // if the duration between last recover and this failure is larger than threshold,
          // start over policy
          if (Duration.between(status.tick.wakeup, now) > threshold)
            status.renewPolicy(serviceParams.servicePolicies.restart)
          else status
        case None => status
      }

      val error = Error(ex)

      tickStatus.next(now) match {
        case None =>
          val cause = ServiceStopCause.ByException(error)
          serviceStop(channel, serviceParams, cause).as(None)
        case Some(nts) =>
          for {
            evt <- servicePanic(channel, serviceParams, nts.tick, error)
            _ <- panicHistory.modify(queue => (queue, queue.add(evt))) // mutable queue
            _ <- F.sleep(nts.tick.snooze.toScala)
          } yield Some(((), nts))
      }
    }

  val stream: Stream[F, Nothing] =
    Stream
      .unfoldEval[F, TickStatus, Unit](
        TickStatus(serviceParams.zerothTick).renewPolicy(serviceParams.servicePolicies.restart)) { status =>
        (serviceReStart(channel, serviceParams, status.tick) <* theService)
          .redeemWith[Option[(Unit, TickStatus)]](
            err => panic(status, err),
            _ => F.pure(None)
          )
      }
      .onFinalizeCase {
        case ExitCase.Succeeded =>
          serviceStop(channel, serviceParams, ServiceStopCause.Successfully)
        case ExitCase.Errored(e) =>
          serviceStop(channel, serviceParams, ServiceStopCause.ByException(Error(e)))
        case ExitCase.Canceled =>
          serviceStop(channel, serviceParams, ServiceStopCause.ByCancellation)
      }
      .drain
}
