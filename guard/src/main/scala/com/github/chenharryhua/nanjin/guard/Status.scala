package com.github.chenharryhua.nanjin.guard

import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

final case class RetriedAction(
  actionName: String,
  alertMask: AlertMask,
  retryPolicy: String,
  actionID: UUID,
  startTime: Instant)

sealed trait Status {
  def applicationName: String
}

sealed trait ServiceStatus extends Status {
  def serviceName: String
  def launchTime: Instant
}

final case class ServiceStarted(applicationName: String, serviceName: String, launchTime: Instant) extends ServiceStatus

final case class ServicePanic(
  applicationName: String,
  serviceName: String,
  retryPolicy: String,
  launchTime: Instant,
  willDelayAndRetry: WillDelayAndRetry,
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
  healthCheckInterval: FiniteDuration,
  launchTime: Instant
) extends ServiceStatus

sealed trait ActionStatus extends Status {
  def applicationName: String
  def retriedAction: RetriedAction
}

final case class ActionRetrying(
  applicationName: String,
  retriedAction: RetriedAction,
  willDelayAndRetry: WillDelayAndRetry,
  error: Throwable
) extends ActionStatus

final case class ActionFailed(
  applicationName: String,
  retriedAction: RetriedAction,
  givingUp: GivingUp,
  notes: String, // description of the action
  error: Throwable
) extends ActionStatus

final case class ActionSucced(
  applicationName: String,
  retriedAction: RetriedAction,
  notes: String, // description of the action
  numRetries: Int // how many retries before success
) extends ActionStatus
