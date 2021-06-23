package com.github.chenharryhua.nanjin.guard

import cats.effect.Sync
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.guard.config.{ServiceConfig, TaskConfig, TaskParams}

/** credit to the excellent retry lib [[https://github.com/cb372/cats-retry]]
  */
final class TaskGuard[F[_]] private (taskConfig: TaskConfig) {
  val params: TaskParams = taskConfig.evalConfig

  def updateConfig(f: TaskConfig => TaskConfig) =
    new TaskGuard[F](f(taskConfig))

  def withHostName(hostName: HostName): TaskGuard[F] =
    updateConfig(_.withHostName(hostName))

  def service(serviceName: String): ServiceGuard[F] =
    new ServiceGuard[F](ServiceConfig(serviceName, params))
}

object TaskGuard {

  def apply[F[_]: Sync](applicationName: String): TaskGuard[F] =
    new TaskGuard[F](TaskConfig(applicationName, HostName.local_host))
}
