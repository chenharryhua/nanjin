package com.github.chenharryhua.nanjin.guard
import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Console
import com.codahale.metrics.MetricFilter
import com.github.chenharryhua.nanjin.common.{HostName, UpdateConfig}
import com.github.chenharryhua.nanjin.common.guard.{ServiceName, TaskName}
import com.github.chenharryhua.nanjin.guard.config.{ServiceConfig, TaskConfig, TaskParams}
import com.github.chenharryhua.nanjin.guard.service.{Agent, ServiceGuard}
import natchez.EntryPoint
import natchez.noop.NoopEntrypoint

import java.time.ZoneId

/** credit to the excellent retry lib [[https://github.com/cb372/cats-retry]]
  */
final class TaskGuard[F[_]: Async] private (taskConfig: TaskConfig, entryPoint: Resource[F, EntryPoint[F]])
    extends UpdateConfig[TaskConfig, TaskGuard[F]] {

  val params: TaskParams = taskConfig.evalConfig

  override def updateConfig(f: Endo[TaskConfig]): TaskGuard[F] =
    new TaskGuard[F](f(taskConfig), entryPoint)

  def withEntryPoint(entryPoint: Resource[F, EntryPoint[F]]) =
    new TaskGuard[F](taskConfig, entryPoint)

  def withEntryPoint(entryPoint: EntryPoint[F]) =
    new TaskGuard[F](taskConfig, Resource.pure(entryPoint))

  def service(serviceName: ServiceName): ServiceGuard[F] =
    new ServiceGuard[F](ServiceConfig(serviceName, params), entryPoint, MetricFilter.ALL, None)

}

object TaskGuard {

  def apply[F[_]: Async](taskName: TaskName): TaskGuard[F] =
    new TaskGuard[F](
      TaskConfig(taskName, HostName.local_host, ZoneId.systemDefault()),
      Resource.pure(NoopEntrypoint[F]()))

  // for repl
  def dummyAgent[F[_]: Async: Console]: F[Agent[F]] =
    apply(TaskName("dummy")).service(ServiceName("dummy")).dummyAgent
}
