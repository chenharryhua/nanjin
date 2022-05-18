package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Ref, Temporal}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.{NJError, NJEvent, ServiceStopCause}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ServicePanic, ServiceStart, ServiceStop}
import fs2.concurrent.Channel

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

final private class ServiceEventPublisher[F[_]](serviceStatus: Ref[F, ServiceStatus], channel: Channel[F, NJEvent])(
  implicit F: Temporal[F]) {

  def serviceReStart: F[Unit] =
    for {
      ts <- F.realTimeInstant
      sp <- serviceStatus.get.map(_.serviceParams)
      now = sp.toZonedDateTime(ts)
      _ <- serviceStatus.update(_.goUp(now))
      _ <- channel.send(ServiceStart(sp, now))
    } yield ()

  def servicePanic(delay: FiniteDuration, ex: Throwable): F[Unit] =
    for {
      ts <- F.realTimeInstant
      sp <- serviceStatus.get.map(_.serviceParams)
      now  = sp.toZonedDateTime(ts)
      next = sp.toZonedDateTime(ts.plus(delay.toJava))
      err  = NJError(ex)
      _ <- serviceStatus.update(_.goPanic(now, next, err))
      _ <- channel.send(ServicePanic(serviceParams = sp, timestamp = now, restartTime = next, error = err))
    } yield ()

  def serviceStop(cause: ServiceStopCause): F[Unit] =
    for {
      sp <- serviceStatus.get.map(_.serviceParams)
      ts <- F.realTimeInstant.map(sp.toZonedDateTime)
      _ <- channel.send(ServiceStop(timestamp = ts, serviceParams = sp, cause = cause))
    } yield ()
}
