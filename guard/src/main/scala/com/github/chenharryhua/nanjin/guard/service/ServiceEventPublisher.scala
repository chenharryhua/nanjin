package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Ref, Temporal}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import retry.RetryDetails

final private class ServiceEventPublisher[F[_]: UUIDGen](
  serviceStatus: Ref[F, ServiceStatus],
  channel: Channel[F, NJEvent])(implicit F: Temporal[F]) {

  def serviceReStart: F[Unit] =
    for {
      ts <- F.realTimeInstant
      us <- serviceStatus.updateAndGet(_.goUp(ts))
      _ <- channel.send(ServiceStart(us.serviceParams, us.serviceParams.toZonedDateTime(ts)))
    } yield ()

  def servicePanic(retryDetails: RetryDetails, ex: Throwable): F[Unit] =
    for {
      ts <- F.realTimeInstant
      err <- UUIDGen.randomUUID[F].map(NJError(_, ex))
      us <- serviceStatus.updateAndGet(_.goDown(ts, retryDetails.upcomingDelay, err))
      _ <- channel.send(ServicePanic(us.serviceParams, us.serviceParams.toZonedDateTime(ts), retryDetails, err))
    } yield ()

  def serviceStop(cause: ServiceStopCause): F[Unit] =
    for {
      ss <- serviceStatus.get
      ts <- F.realTimeInstant.map(ss.serviceParams.toZonedDateTime)
      _ <- channel.send(
        ServiceStop(
          timestamp = ts,
          serviceParams = ss.serviceParams,
          cause = cause
        ))
    } yield ()
}
