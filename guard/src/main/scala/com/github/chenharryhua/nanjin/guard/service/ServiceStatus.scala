package com.github.chenharryhua.nanjin.guard.service

import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError}
import com.github.chenharryhua.nanjin.guard.event.MetricSnapshot.LastCounters

import java.time.{Instant, ZonedDateTime}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

sealed abstract private[guard] class ServiceStatus(
  val serviceParams: ServiceParams,
  val lastCounters: LastCounters,
  val ongoingActionSet: Set[ActionInfo],
  val upcomingRestartTime: Option[ZonedDateTime],
  val isUp: Boolean) {

  def include(action: ActionInfo): ServiceStatus
  def exclude(action: ActionInfo): ServiceStatus

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
  * restarting when upcomingRestart is Some
  */

private[guard] object ServiceStatus {

  final case class Up private[ServiceStatus] (
    override val serviceParams: ServiceParams,
    override val ongoingActionSet: Set[ActionInfo],
    override val lastCounters: LastCounters,
    lastRestartTime: ZonedDateTime,
    lastCrashTime: ZonedDateTime)
      extends ServiceStatus(
        serviceParams = serviceParams,
        ongoingActionSet = ongoingActionSet,
        lastCounters = lastCounters,
        upcomingRestartTime = None,
        isUp = true) {

    override def goUp(now: Instant): Up = copy(lastRestartTime = serviceParams.toZonedDateTime(now))
    override def goDown(now: Instant, upcomingDelay: Option[FiniteDuration], cause: NJError): Down = {
      val zdt = serviceParams.toZonedDateTime(now)
      Down(
        serviceParams = serviceParams,
        lastCounters = lastCounters,
        crashTime = zdt,
        upcomingRestartTime = upcomingDelay.map(fd => zdt.plus(fd.toJava)),
        cause = cause)
    }

    override def include(action: ActionInfo): Up = copy(ongoingActionSet = ongoingActionSet.incl(action))
    override def exclude(action: ActionInfo): Up = copy(ongoingActionSet = ongoingActionSet.excl(action))

    override def updateLastCounters(last: LastCounters): Up = copy(lastCounters = last)
  }

  object Up {
    def apply(serviceParams: ServiceParams): Up =
      Up(
        serviceParams = serviceParams,
        ongoingActionSet = Set.empty[ActionInfo],
        lastCounters = LastCounters.empty,
        lastRestartTime = serviceParams.launchTime,
        lastCrashTime = serviceParams.launchTime
      )
  }

  final case class Down private[ServiceStatus] (
    override val serviceParams: ServiceParams,
    override val lastCounters: LastCounters,
    override val upcomingRestartTime: Option[ZonedDateTime],
    crashTime: ZonedDateTime,
    cause: NJError)
      extends ServiceStatus(
        serviceParams = serviceParams,
        lastCounters = lastCounters,
        upcomingRestartTime = upcomingRestartTime,
        ongoingActionSet = Set.empty[ActionInfo],
        isUp = false) {

    override def goUp(now: Instant): Up =
      Up(
        serviceParams = serviceParams,
        lastCounters = lastCounters,
        ongoingActionSet = Set.empty[ActionInfo],
        lastRestartTime = serviceParams.toZonedDateTime(now),
        lastCrashTime = crashTime
      )
    override def goDown(now: Instant, upcomingDelay: Option[FiniteDuration], cause: NJError): Down = {
      val zdt = serviceParams.toZonedDateTime(now)
      copy(crashTime = zdt, upcomingRestartTime = upcomingDelay.map(fd => zdt.plus(fd.toJava)), cause = cause)
    }

    override def include(action: ActionInfo): Down = this
    override def exclude(action: ActionInfo): Down = this

    override def updateLastCounters(last: LastCounters): Down = copy(lastCounters = last)

  }
}
