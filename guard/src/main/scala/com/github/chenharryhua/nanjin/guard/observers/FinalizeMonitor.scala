package com.github.chenharryhua.nanjin.guard.observers

import cats.Monad
import cats.effect.kernel.{Clock, Ref}
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.functor.toFunctorOps
import cats.syntax.flatMap.toFlatMapOps
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
    case _                => ().pure[F]
  }

  val terminated: F[Chunk[A]] = for {
    ts <- Clock[F].realTimeInstant
    messages <- ref
      .modify(m => Map.empty[UUID, ServiceStart] -> m.values)
      .flatMap(values =>
        Chunk
          .from(values)
          .traverseFilter(ss =>
            translate(
              ServiceStop(
                ss.serviceParams,
                ss.serviceParams.toZonedDateTime(ts),
                ServiceStopCause.ByCancellation))))
  } yield messages
}
