package com.github.chenharryhua.nanjin.guard

import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

final case class ServiceInfo(name: String, healthCheck: FiniteDuration, retryPolicy: String, launchTime: Instant)
final case class ActionInfo(name: String, alertMask: AlertMask, retryPolicy: String, id: UUID, startTime: Instant)

sealed trait Status {
  def applicationName: String
}

sealed trait ServiceStatus extends Status {
  def serviceInfo: ServiceInfo
}

final case class ServiceStarted(applicationName: String, serviceInfo: ServiceInfo) extends ServiceStatus

final case class ServicePanic(
  applicationName: String,
  serviceInfo: ServiceInfo,
  retryDetails: RetryDetails,
  error: Throwable
) extends ServiceStatus

final case class ServiceAbnormalStop(
  applicationName: String,
  serviceInfo: ServiceInfo
) extends ServiceStatus

final case class ServiceHealthCheck(
  applicationName: String,
  serviceInfo: ServiceInfo
) extends ServiceStatus

sealed trait ActionStatus extends Status {
  def actionInfo: ActionInfo
}

final case class ActionRetrying(
  applicationName: String,
  actionInfo: ActionInfo,
  willDelayAndRetry: WillDelayAndRetry,
  error: Throwable
) extends ActionStatus

final case class ActionFailed(
  applicationName: String,
  actionInfo: ActionInfo,
  givingUp: GivingUp,
  notes: String, // description of the action
  error: Throwable
) extends ActionStatus

final case class ActionSucced(
  applicationName: String,
  actionInfo: ActionInfo,
  notes: String, // description of the action
  numRetries: Int // how many retries before success
) extends ActionStatus

final case class ForYouInformation(applicationName: String, message: String) extends Status
