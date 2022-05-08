package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{RefSource, Temporal}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.ActionInfo

import java.time.{Duration, ZonedDateTime}
import java.util.UUID

final class NJRuntimeInfo[F[_]: Temporal] private[service] (serviceStatus: RefSource[F, ServiceStatus]) {

  def upTime: F[Duration] =
    Temporal[F].realTimeInstant.flatMap(now => serviceStatus.get.map(_.serviceParams.upTime(now)))

  def latestCrashDuration: F[Option[Duration]] = serviceStatus.get.map {
    case ServiceStatus.Up(_, _, _, lastRestartAt, lastCrashAt) =>
      Some(Duration.between(lastCrashAt, lastRestartAt))
    case _: ServiceStatus.Down => None
  }

  def latestCrash: F[ZonedDateTime] =
    serviceStatus.get.map(_.fold(_.lastCrashTime, _.crashTime))

  def latestRestart: F[Option[ZonedDateTime]] =
    serviceStatus.get.map(_.fold(u => Some(u.lastRestartTime), _ => None))

  def downCause: F[Option[String]] = serviceStatus.get.map(_.fold(_ => None, d => Some(d.cause.message)))

  def serviceID: F[UUID] = serviceStatus.get.map(_.serviceParams.serviceID)

  def isServiceUp: F[Boolean]   = serviceStatus.get.map(_.isUp)
  def isServiceDown: F[Boolean] = serviceStatus.get.map(_.isDown)

  def pendingActions: F[Set[ActionInfo]] = serviceStatus.get.map(_.ongoingActionSet)
}
