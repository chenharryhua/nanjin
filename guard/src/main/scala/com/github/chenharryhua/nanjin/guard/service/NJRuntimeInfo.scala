package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Clock, RefSource}
import cats.syntax.all.*
import cats.Monad

import java.time.Duration
import java.util.UUID

final class NJRuntimeInfo[F[_]: Clock: Monad] private[service] (serviceStatus: RefSource[F, ServiceStatus]) {

  def upTime: F[Duration] =
    Clock[F].realTimeInstant.flatMap(now => serviceStatus.get.map(_.serviceParams.upTime(now)))

  def downCause: F[Option[String]] = serviceStatus.get.map(_.fold(_ => None, d => Some(d.cause.message)))

  def serviceId: F[UUID] = serviceStatus.get.map(_.serviceParams.serviceId)

  def isServiceUp: F[Boolean]    = serviceStatus.get.map(_.isUp)
  def isServicePanic: F[Boolean] = serviceStatus.get.map(_.isPanic)

}
