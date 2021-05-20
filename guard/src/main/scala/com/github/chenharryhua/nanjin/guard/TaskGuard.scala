package com.github.chenharryhua.nanjin.guard

final class TaskGuard[F[_]] private (
  alertServices: List[AlertService[F]],
  serviceConfig: ServiceConfig,
  actionConfig: ActionConfig
) {

  def withApplicationName(value: String): TaskGuard[F] =
    new TaskGuard[F](alertServices, serviceConfig.withApplicationName(value), actionConfig.withApplicationName(value))

  def withServiceName(value: String): TaskGuard[F] =
    new TaskGuard[F](alertServices, serviceConfig.withServiceName(value), actionConfig.withServiceName(value))

  def addAlertService(value: AlertService[F]): TaskGuard[F] =
    new TaskGuard[F](value :: alertServices, serviceConfig, actionConfig)

  val service: ServiceGuard[F] = new ServiceGuard[F](alertServices, serviceConfig)
  val action: ActionGuard[F]   = new ActionGuard[F](alertServices, actionConfig)
}

object TaskGuard {

  def apply[F[_]]: TaskGuard[F] =
    new TaskGuard[F](List(new LogService[F]), ServiceConfig.default, ActionConfig.default)
}
