package com.github.chenharryhua.nanjin.guard

import cats.effect.Sync

/** credit to the excellent retry lib [[https://github.com/cb372/cats-retry]]
  */
final class TaskGuard[F[_]] private (
  applicationName: String,
  serviceConfig: ServiceConfig,
  groupConfig: GroupConfig,
  actionConfig: ActionConfig
) {

  def updateServiceConfig(f: ServiceConfig => ServiceConfig) =
    new TaskGuard[F](applicationName, f(serviceConfig), groupConfig, actionConfig)

  def updateGroupConfig(f: GroupConfig => GroupConfig) =
    new TaskGuard[F](applicationName, serviceConfig, f(groupConfig), actionConfig)

  def updateActionConfig(f: ActionConfig => ActionConfig) =
    new TaskGuard[F](applicationName, serviceConfig, groupConfig, f(actionConfig))

  def service(serviceName: String): ServiceGuard[F] =
    new ServiceGuard[F](applicationName, serviceName, serviceConfig, actionConfig)

  def group(groupName: String): GroupGuard[F] =
    new GroupGuard[F](applicationName, groupName, groupConfig, actionConfig)

}

object TaskGuard {

  def apply[F[_]: Sync](applicationName: String): TaskGuard[F] =
    new TaskGuard[F](applicationName, ServiceConfig.default, GroupConfig.default, ActionConfig.default)
}
