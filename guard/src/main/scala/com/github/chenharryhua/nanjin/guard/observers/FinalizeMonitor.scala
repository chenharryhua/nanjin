package com.github.chenharryhua.nanjin.guard.observers

import cats.Monad
import cats.effect.kernel.{Clock, Ref}
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.github.chenharryhua.nanjin.guard.event.Event.{ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.{Event, ServiceStopCause}
import fs2.Chunk

import java.util.UUID

final private class FinalizeMonitor[F[_]: Clock: Monad, A](
  translate: Event => F[Option[A]],
  ref: Ref[F, Map[UUID, ServiceStart]]) {
  def monitoring(event: Event): F[Unit] = event match {
    case ss: ServiceStart => ref.update(_.updated(ss.serviceParams.serviceId, ss))
    case ss: ServiceStop  => ref.update(_.removed(ss.serviceParams.serviceId))
    case _                => Monad[F].unit
  }

  val terminated: F[Chunk[A]] = for {
    ts <- Clock[F].realTimeInstant
    messages <- ref.get.flatMap(m =>
      Chunk
        .from(m.values)
        .traverseFilter(ss =>
          translate(
            ServiceStop(
              ss.serviceParams,
              ss.serviceParams.toZonedDateTime(ts),
              ServiceStopCause.ByCancellation))))
  } yield messages
}
