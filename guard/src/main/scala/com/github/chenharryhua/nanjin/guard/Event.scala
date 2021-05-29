package com.github.chenharryhua.nanjin.guard

import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.Instant
import java.util.UUID

final case class ServiceInfo(
  applicationName: String,
  serviceName: String,
  retryPolicy: String,
  launchTime: Instant,
  healthCheck: HealthCheck,
  isNormalStop: Boolean)

final case class ActionInfo(
  applicationName: String,
  serviceName: String,
  actionName: String,
  retryPolicy: String,
  launchTime: Instant,
  alertMask: AlertMask,
  id: UUID)

sealed trait Event

sealed trait ServiceEvent extends Event {
  def serviceInfo: ServiceInfo
}

final case class ServiceStarted(serviceInfo: ServiceInfo) extends ServiceEvent

final case class ServicePanic(
  serviceInfo: ServiceInfo,
  retryDetails: RetryDetails,
  error: Throwable
) extends ServiceEvent

final case class ServiceStopped(
  serviceInfo: ServiceInfo
) extends ServiceEvent

final case class ServiceHealthCheck(
  serviceInfo: ServiceInfo
) extends ServiceEvent

sealed trait ActionEvent extends Event {
  def actionInfo: ActionInfo
}

final case class ActionRetrying(
  actionInfo: ActionInfo,
  willDelayAndRetry: WillDelayAndRetry,
  error: Throwable
) extends ActionEvent

final case class ActionFailed(
  actionInfo: ActionInfo,
  givingUp: GivingUp,
  notes: String, // failure notes
  error: Throwable
) extends ActionEvent

final case class ActionSucced(
  actionInfo: ActionInfo,
  notes: String, // success notes
  numRetries: Int // how many retries before success
) extends ActionEvent

final case class ForYouInformation(applicationName: String, message: String) extends Event
