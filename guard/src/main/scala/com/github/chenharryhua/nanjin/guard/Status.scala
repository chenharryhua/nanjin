package com.github.chenharryhua.nanjin.guard

import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.{Instant, ZoneId}
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

final case class RetriedAction(id: UUID, startTime: Instant, zoneId: ZoneId)

sealed trait Status {
  def applicationName: String
  def serviceName: String
}

sealed trait ServiceStatus extends Status {
  def launchTime: Instant
}

final case class ServiceStarted(applicationName: String, serviceName: String, launchTime: Instant) extends ServiceStatus

final case class ServiceRestarting(
  applicationName: String,
  serviceName: String,
  launchTime: Instant,
  willDelayAndRetry: WillDelayAndRetry,
  retryPolicy: String,
  error: Throwable
) extends ServiceStatus

final case class ServiceAbnormalStop(
  applicationName: String,
  serviceName: String,
  launchTime: Instant,
  error: Throwable
) extends ServiceStatus

final case class ServiceHealthCheck(
  applicationName: String,
  serviceName: String,
  launchTime: Instant,
  healthCheckInterval: FiniteDuration)
    extends ServiceStatus

sealed trait ActionStatus extends Status {
  def applicationName: String
  def serviceName: String
  def action: RetriedAction
  def alertMask: AlertMask
}

final case class ActionRetrying(
  applicationName: String,
  serviceName: String,
  action: RetriedAction,
  alertMask: AlertMask,
  willDelayAndRetry: WillDelayAndRetry,
  retryPolicy: String,
  error: Throwable
) extends ActionStatus

final case class ActionFailed(
  applicationName: String,
  serviceName: String,
  action: RetriedAction,
  alertMask: AlertMask,
  givingUp: GivingUp,
  retryPolicy: String,
  notes: String, // description of the action
  error: Throwable
) extends ActionStatus

final case class ActionSucced(
  applicationName: String,
  serviceName: String,
  action: RetriedAction,
  alertMask: AlertMask,
  notes: String, // description of the action
  retries: Int // how many retries before success
) extends ActionStatus
