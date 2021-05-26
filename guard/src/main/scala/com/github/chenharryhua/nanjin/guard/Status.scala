package com.github.chenharryhua.nanjin.guard

import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.Instant
import java.util.UUID

final case class ServiceInfo(name: String, retryPolicy: String, launchTime: Instant, healthCheck: HealthCheck)
final case class ActionInfo(name: String, retryPolicy: String, launchTime: Instant, alertMask: AlertMask, id: UUID)

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
  notes: String, // failure notes
  error: Throwable
) extends ActionStatus

final case class ActionSucced(
  applicationName: String,
  actionInfo: ActionInfo,
  notes: String, // success notes
  numRetries: Int // how many retries before success
) extends ActionStatus

final case class ForYouInformation(applicationName: String, message: String) extends Status
