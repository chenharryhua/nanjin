package com.github.chenharryhua.nanjin.guard

import cats.effect.Sync
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ServiceConfig}

/** credit to the excellent retry lib [[https://github.com/cb372/cats-retry]]
  */
final class TaskGuard[F[_]] private (
  serviceConfig: ServiceConfig,
  actionConfig: ActionConfig
) {

  def updateConfig(f: ServiceConfig => ServiceConfig) =
    new TaskGuard[F](f(serviceConfig), actionConfig)

  def updateActionConfig(f: ActionConfig => ActionConfig) =
    new TaskGuard[F](serviceConfig, f(actionConfig))

  def service(serviceName: String): ServiceGuard[F] =
    new ServiceGuard[F](serviceName, serviceConfig, actionConfig)
}

object TaskGuard {

  def apply[F[_]: Sync](applicationName: String): TaskGuard[F] =
    new TaskGuard[F](ServiceConfig(applicationName), ActionConfig.default)
}
