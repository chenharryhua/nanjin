package com.github.chenharryhua.nanjin.guard.service

import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.MetricSnapshot.LastCounters
import com.github.chenharryhua.nanjin.guard.event.NJError

import java.time.ZonedDateTime

/** Service has two states:
  *
  * Up
  *
  * Panic
  */
sealed private[guard] trait ServiceStatus {

  def serviceParams: ServiceParams

  /** None: Service is up
    *
    * Some(ts): Service is in panic, will be restarted at ts
    */
  def upcomingRestartTime: Option[ZonedDateTime]

  def lastCounters: LastCounters // latest updated counters. for delta metrics report
  def updateLastCounters(last: LastCounters): ServiceStatus

  def isUp: Boolean
  def timestamp: ZonedDateTime // when updated

  // state change
  private[service] def goUp(now: ZonedDateTime): ServiceStatus.Up
  private[service] def goPanic(
    now: ZonedDateTime,
    restart: ZonedDateTime,
    cause: NJError): ServiceStatus.Panic

  final def isPanic: Boolean = !isUp
  final def fold[A](up: ServiceStatus.Up => A, panic: ServiceStatus.Panic => A): A =
    this match {
      case s: ServiceStatus.Up    => up(s)
      case s: ServiceStatus.Panic => panic(s)
    }
}

/** Up - service is up
  *
  * restarting when upcomingRestart is Some
  */

private[guard] object ServiceStatus {
  private[service] def initialize(serviceParams: ServiceParams): Up =
    Up(serviceParams = serviceParams, lastCounters = LastCounters.empty, timestamp = serviceParams.launchTime)

  final case class Up private[ServiceStatus] (
    serviceParams: ServiceParams,
    lastCounters: LastCounters,
    timestamp: ZonedDateTime)
      extends ServiceStatus {

    override private[service] def goUp(now: ZonedDateTime): Up = this // no-op
    override private[service] def goPanic(
      now: ZonedDateTime,
      restartTime: ZonedDateTime,
      cause: NJError): Panic =
      Panic(
        serviceParams = serviceParams,
        lastCounters = lastCounters,
        timestamp = now,
        plannedRestartTime = restartTime,
        cause = cause)

    override def updateLastCounters(last: LastCounters): Up = copy(lastCounters = last)

    override val upcomingRestartTime: Option[ZonedDateTime] = None
    override val isUp: Boolean                              = true
  }

  final case class Panic private[ServiceStatus] (
    serviceParams: ServiceParams,
    lastCounters: LastCounters,
    timestamp: ZonedDateTime,
    plannedRestartTime: ZonedDateTime,
    cause: NJError)
      extends ServiceStatus {

    override private[service] def goUp(now: ZonedDateTime): Up =
      Up(serviceParams = serviceParams, lastCounters = lastCounters, timestamp = now)

    // no-op
    override private[service] def goPanic(now: ZonedDateTime, restart: ZonedDateTime, cause: NJError): Panic =
      this

    override def updateLastCounters(last: LastCounters): Panic = copy(lastCounters = last)

    override val upcomingRestartTime: Option[ZonedDateTime] = Some(plannedRestartTime)
    override val isUp: Boolean                              = false
  }
}
