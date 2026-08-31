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

final private class FinalizeMonitor[F[_]: {Clock, Monad}, A](
  translate: Event => F[Option[A]],
  ref: Ref[F, Map[ServiceId, ServiceStart]]) {
  def monitoring(event: Event): F[Unit] = event match {
    case ss: ServiceStart => ref.update(_.updated(ss.serviceIdentity.serviceId, ss))
    case ss: ServiceStop  => ref.update(_.removed(ss.serviceIdentity.serviceId))
    case _                => ().pure[F]
  }

  val terminated: F[Chunk[A]] = for {
    ts <- Clock[F].realTimeInstant
    messages <- ref
      .modify(m => Map.empty[ServiceId, ServiceStart] -> m.values)
      .flatMap(values =>
        Chunk
          .from(values)
          .traverseFilter(ss =>
            translate(
              ServiceStop(
                ss.serviceIdentity,
                ss.policy,
                ss.brief,
                ss.serviceIdentity.toTimestamp(ts),
                StopReason.ByCancellation))))
  } yield messages
}
