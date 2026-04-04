package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Async
import cats.effect.syntax.monadCancel.given
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.monadError.given
import cats.syntax.order.given
import com.github.chenharryhua.nanjin.common.chrono.PolicyTick
import com.github.chenharryhua.nanjin.guard.config.RestartPolicy
import com.github.chenharryhua.nanjin.guard.event.{StackTrace, StopReason}
import fs2.Stream
import org.typelevel.cats.time.instances.duration.given

import java.time.Duration
import scala.jdk.DurationConverters.JavaDurationOps

private object Watchdog:
  def apply[F[_]](theService: F[Unit], handler: ServiceEventHandler[F])(using
    F: Async[F]): Stream[F, Nothing] = {

    val rp: RestartPolicy = handler.serviceParams.servicePolicies.restart

    def panic(status: PolicyTick[F], ex: Throwable): F[Option[(Unit, PolicyTick[F])]] =
      F.realTimeInstant.flatMap[Option[(Unit, PolicyTick[F])]] { now =>
        val tickStatus: PolicyTick[F] =
          rp.threshold match
            case Some(threshold) =>
              // if the duration between the last tick's conclude time and this failure is larger than threshold,
              // start over policy
              if Duration.between(status.tick.conclude, now) > threshold
              then status.renewPolicy(rp.policy)
              else status
            case None => status

        val stackTrace: StackTrace = StackTrace(ex)

        tickStatus.next(now).flatMap {
          case None      => handler.serviceStop(StopReason.ByException(stackTrace)).as(None)
          case Some(nts) =>
            for {
              _ <- handler.servicePanic(nts.tick, stackTrace)
              _ <- F.sleep(nts.tick.snooze.toScala)
            } yield Some(((), nts))
        }
      }

    Stream.eval(PolicyTick.zeroth[F](handler.serviceParams.zoneId, rp.policy))
      .flatMap {
        Stream
          .unfoldEval[F, PolicyTick[F], Unit](_) { status =>
            (handler.serviceStart(status.tick) <* theService)
              .redeemWith[Option[(Unit, PolicyTick[F])]](
                err => panic(status, err),
                _ => handler.serviceStop(StopReason.Successfully).as(None)
              )
              .onCancel(handler.serviceCancel)
          }
          .drain
      }
  }
end Watchdog
