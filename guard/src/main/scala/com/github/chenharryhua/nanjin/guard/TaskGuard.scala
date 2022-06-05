package com.github.chenharryhua.nanjin.guard
import cats.Endo
import cats.effect.kernel.Async
import com.codahale.metrics.MetricFilter
import com.github.chenharryhua.nanjin.common.guard.{ServiceName, TaskName}
import com.github.chenharryhua.nanjin.common.{HostName, UpdateConfig}
import com.github.chenharryhua.nanjin.guard.config.{ServiceConfig, TaskConfig, TaskParams}
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard

import java.time.ZoneId

/** credit to the excellent retry lib [[https://github.com/cb372/cats-retry]]
  */
final class TaskGuard[F[_]: Async] private (taskConfig: TaskConfig)
    extends UpdateConfig[TaskConfig, TaskGuard[F]] {

  val params: TaskParams = taskConfig.evalConfig

  override def updateConfig(f: Endo[TaskConfig]): TaskGuard[F] =
    new TaskGuard[F](f(taskConfig))

  def service(serviceName: ServiceName): ServiceGuard[F] =
    new ServiceGuard[F](ServiceConfig(serviceName, params), MetricFilter.ALL, None)

}

object TaskGuard {

  def apply[F[_]: Async](taskName: TaskName): TaskGuard[F] =
    new TaskGuard[F](TaskConfig(taskName, HostName.local_host, ZoneId.systemDefault()))
}
