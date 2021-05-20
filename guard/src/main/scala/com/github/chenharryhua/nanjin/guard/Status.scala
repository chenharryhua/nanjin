package com.github.chenharryhua.nanjin.guard

import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.util.UUID

final case class ActionID(value: UUID) extends AnyVal

final case class ActionInput(value: String) extends AnyVal

sealed trait Status {
  def applicationName: ApplicationName
  def serviceName: ServiceName
}

sealed trait ServiceStatus extends Status

final case class ServiceStarted(applicationName: ApplicationName, serviceName: ServiceName) extends ServiceStatus

final case class ServiceRestarting(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  willDelayAndRetry: WillDelayAndRetry,
  error: Throwable
) extends ServiceStatus

final case class ServiceAbnormalStop(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  error: Throwable
) extends ServiceStatus

final case class ServiceHealthCheck(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  healthCheckInterval: HealthCheckInterval)
    extends ServiceStatus

sealed trait ActionStatus extends Status {
  def actionID: ActionID
  def actionInput: ActionInput
  def alertMask: AlertMask
}

final case class ActionRetrying(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  actionID: ActionID,
  actionInput: ActionInput,
  alertMask: AlertMask,
  error: Throwable,
  willDelayAndRetry: WillDelayAndRetry
) extends ActionStatus

final case class ActionFailed(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  actionID: ActionID,
  actionInput: ActionInput,
  alertMask: AlertMask,
  error: Throwable,
  givingUp: GivingUp)
    extends ActionStatus

final case class ActionSucced(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  actionID: ActionID,
  actionInput: ActionInput,
  alertMask: AlertMask)
    extends ActionStatus
