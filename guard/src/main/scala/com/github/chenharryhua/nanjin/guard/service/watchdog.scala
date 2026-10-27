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
import com.github.chenharryhua.nanjin.guard.config.StackTrace
import com.github.chenharryhua.nanjin.guard.event.StopReason
import fs2.Stream
import org.typelevel.cats.time.instances.duration.given

import java.time.Duration
import scala.jdk.DurationConverters.JavaDurationOps

/** The service supervision and restart loop. Despite the name this is the primary run loop, not a passive
  * liveness watcher: it runs the user's service effect, applies the restart policy on failure, and decides
  * every terminal `StopReason`.
  *
  * Each iteration emits `serviceStart` for the current tick and then runs `theService`. Its outcome drives
  * the loop:
  *   - '''success''' — the effect completes: stop with `StopReason.Successfully` and end the loop.
  *   - '''failure''' — the effect throws: in `panic`, first renew the restart policy when the service ran
  *     longer than `RestartPolicy.threshold` (so a long-healthy run resets the backoff), then ask the
  *     `PolicyTick` for the next tick. `None` means the policy is exhausted, so stop with
  *     `StopReason.ByException`; `Some` means emit a `ServicePanic`, sleep the tick's snooze, and retry.
  *   - '''cancellation''' — the loop is canceled (e.g. the enclosing `eventStream` is interrupted): the
  *     `onCancel` finalizer runs `serviceCancel`, which stops with `StopReason.ByCancellation` unless the
  *     channel is already closed by a terminal stop.
  *
  * The seed `PolicyTick` comes from the restart policy; an empty policy (the default) yields no next tick, so
  * the first failure stops with `ByException` and no restart occurs. The loop runs as a `.concurrently`
  * producer in `ServiceGuard.eventStream`, so closing the event channel on any terminal stop ends the main
  * stream and cancels this loop.
  */
private def watchdog[F[_]](theService: F[Unit], handler: ServiceEventHandler[F])(using
  F: Async[F]): Stream[F, Nothing] = {

  val rp: RestartPolicy = handler.serviceParams.policies.restart

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

  Stream.eval(PolicyTick.seed[F](handler.serviceParams.serviceIdentity.launchTime.zoneId, rp.policy))
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
