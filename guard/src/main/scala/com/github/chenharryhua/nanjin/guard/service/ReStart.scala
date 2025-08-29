package com.github.chenharryhua.nanjin.guard.service

import cats.effect.implicits.monadCancelOps_
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
  panicHistory: AtomicCell[F, CircularFifoQueue[ServicePanic]],
  eventLogger: EventLogger[F],
  theService: F[Unit])
    extends duration {
  private val serviceParams: ServiceParams = eventLogger.serviceParams

  private[this] val F = Temporal[F]

  private[this] def stop(cause: ServiceStopCause): F[Unit] =
    serviceStop(channel, eventLogger, cause)

  private[this] def panic(status: TickStatus, ex: Throwable): F[Option[(Unit, TickStatus)]] =
    F.realTimeInstant.flatMap[Option[(Unit, TickStatus)]] { now =>
      val tickStatus: TickStatus = serviceParams.servicePolicies.restart.threshold match {
        case Some(threshold) =>
          // if the duration between last recover and this failure is larger than threshold,
          // start over policy
          if (Duration.between(status.tick.conclude, now) > threshold)
            status.renewPolicy(serviceParams.servicePolicies.restart.policy)
          else status
        case None => status
      }

      val error: Error = Error(ex)

      tickStatus.next(now) match {
        case None      => stop(ServiceStopCause.ByException(error)).as(None)
        case Some(nts) =>
          for {
            evt <- servicePanic(channel, eventLogger, nts.tick, error)
            _ <- panicHistory.modify(queue => (queue, queue.add(evt))) // mutable queue
            _ <- F.sleep(nts.tick.snooze.toScala)
          } yield Some(((), nts))
      }
    }

  val stream: Stream[F, Nothing] =
    Stream
      .unfoldEval[F, TickStatus, Unit](
        TickStatus(serviceParams.zerothTick).renewPolicy(serviceParams.servicePolicies.restart.policy)) {
        status =>
          (serviceStart(channel, eventLogger, status.tick) <* theService)
            .redeemWith[Option[(Unit, TickStatus)]](
              err => panic(status, err),
              _ => stop(ServiceStopCause.Successfully).as(None)
            )
            .onCancel(channel.isClosed.ifM(F.unit, stop(ServiceStopCause.ByCancellation)))
      }
      .drain
}
