package com.github.chenharryhua.nanjin.guard.service

import cats.effect.implicits.monadCancelOps_
import cats.effect.kernel.Async
import cats.syntax.apply.catsSyntaxApplyOps
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.monadError.catsSyntaxMonadError
import cats.syntax.order.catsSyntaxPartialOrder
import com.github.chenharryhua.nanjin.common.chrono.PolicyTick
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{StackTrace, StopReason}
import fs2.Stream
import org.typelevel.cats.time.instances.duration

import java.time.Duration
import scala.jdk.DurationConverters.JavaDurationOps

final private class Watchdog[F[_]: Async](theService: F[Unit], lifecyclePublisher: LifecyclePublisher[F])
    extends duration {

  private[this] val F = Async[F]
  private[this] val serviceParams: ServiceParams = lifecyclePublisher.serviceParams

  private[this] def panic(status: PolicyTick[F], ex: Throwable): F[Option[(Unit, PolicyTick[F])]] =
    F.realTimeInstant.flatMap[Option[(Unit, PolicyTick[F])]] { now =>
      val tickStatus: PolicyTick[F] =
        serviceParams.servicePolicies.restart.threshold match {
          case Some(threshold) =>
            // if the duration between last recover and this failure is larger than threshold,
            // start over policy
            if (Duration.between(status.tick.conclude, now) > threshold)
              status.renewPolicy(serviceParams.servicePolicies.restart.policy)
            else status
          case None => status
        }

      val stackTrace: StackTrace = StackTrace(ex)

      tickStatus.next(now).flatMap {
        case None      => lifecyclePublisher.service_stop(StopReason.ByException(stackTrace)).as(None)
        case Some(nts) =>
          for {
            _ <- lifecyclePublisher.service_panic(nts.tick, stackTrace)
            _ <- F.sleep(nts.tick.snooze.toScala)
          } yield Some(((), nts))
      }
    }

  private val stream: Stream[F, Nothing] =
    Stream
      .eval(PolicyTick.zeroth[F](serviceParams.zoneId, serviceParams.servicePolicies.restart.policy))
      .flatMap {
        Stream
          .unfoldEval[F, PolicyTick[F], Unit](_) { status =>
            (lifecyclePublisher.service_start(status.tick) <* theService)
              .redeemWith[Option[(Unit, PolicyTick[F])]](
                err => panic(status, err),
                _ => lifecyclePublisher.service_stop(StopReason.Successfully).as(None)
              )
              .onCancel(lifecyclePublisher.service_cancel)
          }
          .drain
      }
}

private object Watchdog {
  def stream[F[_]: Async](
    theService: F[Unit],
    lifecyclePublisher: LifecyclePublisher[F]): Stream[F, Nothing] =
    new Watchdog[F](theService, lifecyclePublisher).stream
}
