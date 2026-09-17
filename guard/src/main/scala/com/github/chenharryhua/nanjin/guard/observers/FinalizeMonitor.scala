package com.github.chenharryhua.nanjin.guard.observers

import cats.Monad
import cats.effect.kernel.{Clock, Ref}
import cats.syntax.applicative.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.guard.config.ServiceId
import com.github.chenharryhua.nanjin.guard.event.Event.{ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.{Event, StopReason}
import fs2.Chunk

/** Tracks which services have started but not yet cleanly stopped, so that an abrupt shutdown can synthesize
  * a `ServiceStop` for each one still running.
  *
  * This is a reusable building block for writing observers: feed every event to `monitoring`, and run
  * `terminated` in the stream's finalizer to obtain the synthesized `ServiceStop`s for services that never
  * emitted their own stop (e.g. on cancellation). Construct one with `FinalizeMonitor[F]`.
  *
  * Translation is intentionally not this monitor's concern: `terminated` hands back the synthesized
  * `ServiceStop` events, and each observer runs them through its own `Translator` in the finalizer, mirroring
  * how it translates events on the main stream. This keeps the event available to callers that need to derive
  * per-event data (e.g. an idempotency key) from it.
  */
final class FinalizeMonitor[F[_]: {Clock, Monad}] private (ref: Ref[F, Map[ServiceId, ServiceStart]]) {
  def monitoring(event: Event): F[Unit] = event match {
    case ss: ServiceStart => ref.update(_.updated(ss.serviceIdentity.serviceId, ss))
    case ss: ServiceStop  => ref.update(_.removed(ss.serviceIdentity.serviceId))
    case _                => ().pure[F]
  }

  /** The synthesized `ServiceStop` (by cancellation) for every service still running at shutdown. */
  val terminated: F[Chunk[Event]] = for {
    ts <- Clock[F].realTimeInstant
    stops <- ref
      .modify(m => Map.empty[ServiceId, ServiceStart] -> m.values)
      .map(values =>
        Chunk.from(values).map { ss =>
          ServiceStop(
            ss.serviceIdentity,
            None,
            ss.policy,
            ss.brief,
            ss.serviceIdentity.toTimestamp(ts),
            StopReason.ByCancellation)
        })
  } yield stops
}

object FinalizeMonitor {

  /** Build a fresh `FinalizeMonitor` with its own empty tracking state. */
  def apply[F[_]: {Clock, Monad}](using F: Ref.Make[F]): F[FinalizeMonitor[F]] =
    F.refOf(Map.empty[ServiceId, ServiceStart]).map(new FinalizeMonitor(_))
}
