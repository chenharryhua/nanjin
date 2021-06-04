package com.github.chenharryhua.nanjin.guard

import cats.effect.Sync
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ServiceConfig}

/** credit to the excellent retry lib [[https://github.com/cb372/cats-retry]]
  */
final class TaskGuard[F[_]] private (
  applicationName: String,
  serviceConfig: ServiceConfig,
  actionConfig: ActionConfig
) {

  def updateServiceConfig(f: ServiceConfig => ServiceConfig) =
    new TaskGuard[F](applicationName, f(serviceConfig), actionConfig)

  def updateActionConfig(f: ActionConfig => ActionConfig) =
    new TaskGuard[F](applicationName, serviceConfig, f(actionConfig))

  def service(serviceName: String): ServiceGuard[F] =
    new ServiceGuard[F](applicationName, serviceName, serviceConfig, actionConfig)
}

object TaskGuard {

  def apply[F[_]: Sync](applicationName: String): TaskGuard[F] =
    new TaskGuard[F](applicationName, ServiceConfig.default, ActionConfig.default)
}
