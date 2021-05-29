package com.github.chenharryhua.nanjin.guard

import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.Instant
import java.util.UUID

final case class ServiceInfo(applicationName: String, serviceName: String, params: ServiceParams, launchTime: Instant)

final case class ActionInfo(
  applicationName: String,
  serviceName: String,
  actionName: String,
  params: ActionParams,
  launchTime: Instant,
  id: UUID)

sealed trait NJEvent

sealed trait ServiceEvent extends NJEvent {
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

sealed trait ActionEvent extends NJEvent {
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

final case class ForYouInformation(applicationName: String, message: String) extends NJEvent
