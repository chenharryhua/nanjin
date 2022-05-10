package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Ref, Temporal}
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, ServiceStart, ServiceStop, ServiceStopCause}
import com.github.chenharryhua.nanjin.guard.translators.Translator
import cats.syntax.all.*
import java.util.UUID

final private class ObserverFinalizeMonitor[F[_], A](
  translator: Translator[F, A],
  ref: Ref[F, Map[UUID, ServiceStart]])(implicit F: Temporal[F]) {
  def monitoring(event: NJEvent): F[Unit] = event match {
    case ss: ServiceStart => ref.update(_.updated(ss.serviceID, ss))
    case ss: ServiceStop  => ref.update(_.removed(ss.serviceID))
    case _                => F.unit
  }

  val terminated: F[List[A]] = for {
    ts <- F.realTimeInstant
    msgs <- ref.get.flatMap(
      _.values.toList.traverse(ss =>
        translator.translate(
          ServiceStop(
            ss.serviceParams,
            ss.serviceParams.toZonedDateTime(ts),
            ServiceStopCause.Abnormally("external termination")))))
  } yield msgs.flatten
}
