package com.github.chenharryhua.nanjin.guard

import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.util.UUID

final private case class ApplicationName(value: String) extends AnyVal
final private case class ServiceName(value: String) extends AnyVal

final private case class ActionID(value: UUID) extends AnyVal

final private case class ActionInput(value: String) extends AnyVal

sealed private trait Status {
  def applicationName: ApplicationName
  def serviceName: ServiceName
}

sealed private trait ServiceStatus extends Status

final private case class ServiceStarted(applicationName: ApplicationName, serviceName: ServiceName)
    extends ServiceStatus

final private case class ServiceRestarting(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  willDelayAndRetry: WillDelayAndRetry,
  alertEveryNRetry: AlertEveryNRetries,
  error: Throwable
) extends ServiceStatus

final private case class ServiceAbnormalStop(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  error: Throwable
) extends ServiceStatus

final private case class ServiceHealthCheck(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  healthCheckInterval: HealthCheckInterval)
    extends ServiceStatus

sealed private trait ActionStatus extends Status {
  def actionID: ActionID
  def actionInput: ActionInput
  def alertLevel: AlertLevel
}

final private case class ActionRetrying(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  actionID: ActionID,
  actionInput: ActionInput,
  error: Throwable,
  willDelayAndRetry: WillDelayAndRetry,
  alertLevel: AlertLevel)
    extends ActionStatus

final private case class ActionFailed(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  actionID: ActionID,
  actionInput: ActionInput,
  error: Throwable,
  givingUp: GivingUp,
  alertLevel: AlertLevel)
    extends ActionStatus

final private case class ActionSucced(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  actionID: ActionID,
  actionInput: ActionInput,
  alertLevel: AlertLevel)
    extends ActionStatus
