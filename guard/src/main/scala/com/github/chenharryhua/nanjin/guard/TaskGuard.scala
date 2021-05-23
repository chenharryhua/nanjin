package com.github.chenharryhua.nanjin.guard

/** credit to the excellent retry lib [[https://github.com/cb372/cats-retry]]
  */
final class TaskGuard[F[_]] private (
  alertServices: List[AlertService[F]],
  serviceConfig: ServiceConfig,
  actionConfig: ActionConfig
) {

  def withApplicationName(value: String): TaskGuard[F] =
    new TaskGuard[F](alertServices, serviceConfig.withApplicationName(value), actionConfig.withApplicationName(value))

  def addAlertService(value: AlertService[F]): TaskGuard[F] =
    new TaskGuard[F](value :: alertServices, serviceConfig, actionConfig)

  val service: ServiceGuard[F] = new ServiceGuard[F](alertServices, serviceConfig)
  val action: ActionGuard[F]   = new ActionGuard[F](alertServices, actionConfig)
}

object TaskGuard {

  def apply[F[_]]: TaskGuard[F] =
    new TaskGuard[F](List(new LogService[F]), ServiceConfig.default, ActionConfig.default)
}
