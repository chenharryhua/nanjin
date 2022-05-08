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
      ss <- serviceStatus.updateAndGet(_.goUp(ts))
      _ <- channel.send(ServiceStart(ss.serviceParams, ss.serviceParams.toZonedDateTime(ts)))
    } yield ()

  def servicePanic(retryDetails: RetryDetails, ex: Throwable): F[Unit] =
    for {
      ts <- F.realTimeInstant
      err <- UUIDGen.randomUUID[F].map(NJError(_, ex))
      ss <- serviceStatus.updateAndGet(_.goDown(ts, retryDetails.upcomingDelay, err))
      _ <- channel.send(ServicePanic(ss.serviceParams, ss.serviceParams.toZonedDateTime(ts), retryDetails, err))
    } yield ()

  def serviceStop(cause: ServiceStopCause): F[Unit] =
    for {
      sp <- serviceStatus.get.map(_.serviceParams)
      ts <- F.realTimeInstant.map(sp.toZonedDateTime)
      _ <- channel.send(ServiceStop(timestamp = ts, serviceParams = sp, cause = cause))
    } yield ()
}
