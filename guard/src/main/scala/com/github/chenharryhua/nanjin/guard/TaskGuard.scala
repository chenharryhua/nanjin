package com.github.chenharryhua.nanjin.guard
import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Console
import com.github.chenharryhua.nanjin.common.{HostName, UpdateConfig}
import com.github.chenharryhua.nanjin.common.guard.{ServiceName, TaskName}
import com.github.chenharryhua.nanjin.guard.config.{ServiceConfig, TaskConfig, TaskParams}
import com.github.chenharryhua.nanjin.guard.service.{Agent, ServiceGuard}
import natchez.EntryPoint
import natchez.noop.NoopEntrypoint
import retry.RetryPolicies

import java.time.ZoneId

/** poor man's telemetry
  */
final class TaskGuard[F[_]: Async] private (taskConfig: TaskConfig, entryPoint: Resource[F, EntryPoint[F]])
    extends UpdateConfig[TaskConfig, TaskGuard[F]] {

  val params: TaskParams = taskConfig.evalConfig

  override def updateConfig(f: Endo[TaskConfig]): TaskGuard[F] =
    new TaskGuard[F](f(taskConfig), entryPoint)

  def withEntryPoint(ep: Resource[F, EntryPoint[F]]): TaskGuard[F] =
    new TaskGuard[F](taskConfig, ep)

  def withEntryPoint(ep: EntryPoint[F]): TaskGuard[F] =
    withEntryPoint(Resource.pure[F, EntryPoint[F]](ep))

  def service(serviceName: ServiceName): ServiceGuard[F] =
    new ServiceGuard[F](
      serviceConfig = ServiceConfig(serviceName, params),
      metricSet = Nil,
      jmxBuilder = None,
      entryPoint = entryPoint,
      restartPolicy = RetryPolicies.alwaysGiveUp
    )
}

object TaskGuard {

  def apply[F[_]: Async](taskName: TaskName): TaskGuard[F] =
    new TaskGuard[F](
      TaskConfig(taskName, HostName.local_host, ZoneId.systemDefault()),
      Resource.pure(NoopEntrypoint[F]()))

  // for repl
  def dummyAgent[F[_]: Async: Console]: Resource[F, Agent[F]] =
    apply(TaskName("dummy")).service(ServiceName("dummy")).dummyAgent
}
