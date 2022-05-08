package com.github.chenharryhua.nanjin.guard.service

import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError}
import com.github.chenharryhua.nanjin.guard.event.MetricSnapshot.LastCounters

import java.time.{Instant, ZonedDateTime}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

sealed private[guard] trait ServiceStatus {
  def serviceParams: ServiceParams
  def isUp: Boolean

  def upcommingRestart: Option[ZonedDateTime] // None when service is up

  def ongoingActions: Set[ActionInfo]
  def include(action: ActionInfo): ServiceStatus
  def exclude(action: ActionInfo): ServiceStatus

  def lastCounters: LastCounters
  def updateLastCounters(last: LastCounters): ServiceStatus

  def goUp(now: Instant): ServiceStatus
  def goDown(now: Instant, upcomingDelay: Option[FiniteDuration], cause: NJError): ServiceStatus

  final def isDown: Boolean = !isUp
  final def fold[A](up: ServiceStatus.Up => A, down: ServiceStatus.Down => A): A =
    this match {
      case s: ServiceStatus.Up   => up(s)
      case s: ServiceStatus.Down => down(s)
    }
}

/** Up - service is up
  *
  * Down: Stopped when upcommingRestart is None
  *
  * restarting when upcommingRestart is Some
  */

private[guard] object ServiceStatus {

  final case class Up(
    serviceParams: ServiceParams,
    lastCounters: LastCounters,
    ongoingActions: Set[ActionInfo],
    lastRestartAt: ZonedDateTime,
    lastCrashAt: ZonedDateTime)
      extends ServiceStatus {
    override val isUp: Boolean = true

    override def goUp(now: Instant): Up = copy(lastRestartAt = serviceParams.toZonedDateTime(now))
    override def goDown(now: Instant, upcomingDelay: Option[FiniteDuration], cause: NJError): Down = {
      val zdt = serviceParams.toZonedDateTime(now)
      Down(serviceParams, lastCounters, zdt, upcomingDelay.map(fd => zdt.plus(fd.toJava)), cause)
    }

    override val upcommingRestart: Option[ZonedDateTime]    = None
    override def include(action: ActionInfo): ServiceStatus = copy(ongoingActions = ongoingActions.incl(action))
    override def exclude(action: ActionInfo): ServiceStatus = copy(ongoingActions = ongoingActions.excl(action))

    override def updateLastCounters(last: LastCounters): ServiceStatus =
      copy(lastCounters = last)
  }

  object Up {
    def apply(serviceParams: ServiceParams): ServiceStatus =
      Up(serviceParams, LastCounters.empty, Set.empty[ActionInfo], serviceParams.launchTime, serviceParams.launchTime)
  }

  final case class Down(
    serviceParams: ServiceParams,
    lastCounters: LastCounters,
    crashAt: ZonedDateTime,
    upcommingRestart: Option[ZonedDateTime],
    cause: NJError)
      extends ServiceStatus {

    override val isUp: Boolean = false

    override def goUp(now: Instant): Up =
      Up(serviceParams, lastCounters, Set.empty[ActionInfo], serviceParams.toZonedDateTime(now), crashAt)
    override def goDown(now: Instant, upcomingDelay: Option[FiniteDuration], cause: NJError): Down = {
      val zdt = serviceParams.toZonedDateTime(now)
      this.copy(crashAt = zdt, upcommingRestart = upcomingDelay.map(fd => zdt.plus(fd.toJava)), cause = cause)
    }

    override val ongoingActions: Set[ActionInfo] = Set.empty[ActionInfo]

    override def include(action: ActionInfo): ServiceStatus = this
    override def exclude(action: ActionInfo): ServiceStatus = this

    override def updateLastCounters(last: LastCounters): ServiceStatus = copy(lastCounters = last)
  }
}
