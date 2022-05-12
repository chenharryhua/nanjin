package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{RefSource, Temporal}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.ActionInfo

import java.time.Duration
import java.util.UUID

final class NJRuntimeInfo[F[_]: Temporal] private[service] (serviceStatus: RefSource[F, ServiceStatus]) {

  def upTime: F[Duration] =
    Temporal[F].realTimeInstant.flatMap(now => serviceStatus.get.map(_.serviceParams.upTime(now)))

  def downCause: F[Option[String]] = serviceStatus.get.map(_.fold(_ => None, d => Some(d.cause.message)))

  def serviceID: F[UUID] = serviceStatus.get.map(_.serviceParams.serviceID)

  def isServiceUp: F[Boolean]    = serviceStatus.get.map(_.isUp)
  def isServicePanic: F[Boolean] = serviceStatus.get.map(_.isPanic)

  def pendingActions: F[Set[ActionInfo]] = serviceStatus.get.map(_.ongoingActionSet)
}
