package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Async
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.{HostName, UpdateConfig}
import com.github.chenharryhua.nanjin.guard.config.{ServiceConfig, TaskConfig, TaskParams}

/** credit to the excellent retry lib [[https://github.com/cb372/cats-retry]]
  */
final class TaskGuard[F[_]: Async] private (taskConfig: TaskConfig) extends UpdateConfig[TaskConfig, TaskGuard[F]] {

  val params: TaskParams = taskConfig.evalConfig

  override def updateConfig(f: TaskConfig => TaskConfig): TaskGuard[F] =
    new TaskGuard[F](f(taskConfig))

  def service(serviceName: String): ServiceGuard[F] =
    new ServiceGuard[F](ServiceConfig(serviceName, params), None)

}

object TaskGuard {

  def apply[F[_]: Async](applicationName: String): TaskGuard[F] =
    new TaskGuard[F](TaskConfig(applicationName, HostName.local_host))
}
