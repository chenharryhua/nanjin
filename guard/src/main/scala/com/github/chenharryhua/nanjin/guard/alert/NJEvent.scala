package com.github.chenharryhua.nanjin.guard.alert

import cats.Show
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ServiceParams}
import org.apache.commons.lang3.exception.ExceptionUtils
import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.Instant
import java.util.UUID

final case class ServiceInfo(applicationName: String, serviceName: String, params: ServiceParams, launchTime: Instant) {
  def metricsKey: String = s"service.$applicationName.$serviceName"
}

final case class ActionInfo(
  applicationName: String,
  serviceName: String,
  actionName: String,
  params: ActionParams,
  id: UUID,
  launchTime: Instant) {
  def metricsKey: String = s"action.$applicationName.$serviceName.$actionName"
}

final case class Notes private (value: String)

object Notes {
  def apply(str: String): Notes = new Notes(Option(str).getOrElse("null in notes"))
}

sealed trait NJEvent

object NJEvent {
  implicit private val showInstant: Show[Instant]     = _.toString()
  implicit private val showThrowable: Show[Throwable] = ex => ExceptionUtils.getMessage(ex)
  implicit val showNJEvent: Show[NJEvent]             = cats.derived.semiauto.show[NJEvent]
}

sealed trait ServiceEvent extends NJEvent {
  def serviceInfo: ServiceInfo
}

final case class ServiceStarted(serviceInfo: ServiceInfo) extends ServiceEvent

final case class ServicePanic(
  serviceInfo: ServiceInfo,
  retryDetails: RetryDetails,
  errorID: UUID,
  error: Throwable
) extends ServiceEvent

final case class ServiceStoppedAbnormally(
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
  endAt: Instant, // computation finished
  notes: Notes, // failure notes
  error: Throwable
) extends ActionEvent

final case class ActionSucced(
  actionInfo: ActionInfo,
  endAt: Instant, // computation finished
  numRetries: Int, // how many retries before success
  notes: Notes // success notes
) extends ActionEvent

final case class ForYouInformation(applicationName: String, message: String) extends NJEvent
