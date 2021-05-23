package com.github.chenharryhua.nanjin.guard

import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

final case class RetriedAction(id: UUID, startTime: Instant)
final case class Notes(value: String) extends AnyVal
final case class RetryPolicyText(value: String) extends AnyVal
final case class NumberOfRetries(value: Int) extends AnyVal

sealed trait Status {
  def applicationName: String
  def serviceName: String
}

sealed trait ServiceStatus extends Status {
  def launchTime: Instant
}

final case class ServiceStarted(applicationName: String, serviceName: String, launchTime: Instant) extends ServiceStatus

final case class ServicePanic(
  applicationName: String,
  serviceName: String,
  launchTime: Instant,
  willDelayAndRetry: WillDelayAndRetry,
  retryPolicy: RetryPolicyText,
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
  retryPolicy: RetryPolicyText,
  error: Throwable
) extends ActionStatus

final case class ActionFailed(
  applicationName: String,
  serviceName: String,
  action: RetriedAction,
  alertMask: AlertMask,
  givingUp: GivingUp,
  retryPolicy: RetryPolicyText,
  notes: Notes, // description of the action
  error: Throwable
) extends ActionStatus

final case class ActionSucced(
  applicationName: String,
  serviceName: String,
  action: RetriedAction,
  alertMask: AlertMask,
  notes: Notes, // description of the action
  retries: NumberOfRetries // how many retries before success
) extends ActionStatus
