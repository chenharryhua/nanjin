package com.github.chenharryhua.nanjin.guard
import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Console
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.{ServiceName, TaskConfig}
import com.github.chenharryhua.nanjin.guard.service.{GeneralAgent, ServiceGuard}
import fs2.io.net.Network
import natchez.EntryPoint
import natchez.noop.NoopEntrypoint

/** poor man's telemetry
  */
final class TaskGuard[F[_]: Async: Network] private (
  taskConfig: TaskConfig,
  entryPoint: Resource[F, EntryPoint[F]])
    extends UpdateConfig[TaskConfig, TaskGuard[F]] {

  override def updateConfig(f: Endo[TaskConfig]): TaskGuard[F] =
    new TaskGuard[F](f(taskConfig), entryPoint)

  def withEntryPoint(ep: Resource[F, EntryPoint[F]]): TaskGuard[F] = new TaskGuard[F](taskConfig, ep)
  def withEntryPoint(ep: EntryPoint[F]): TaskGuard[F] = withEntryPoint(Resource.pure[F, EntryPoint[F]](ep))

  def service(serviceName: String): ServiceGuard[F] =
    new ServiceGuard[F](
      serviceName = ServiceName(serviceName),
      taskParams = taskConfig.evalConfig,
      config = identity,
      entryPoint = entryPoint,
      jmxBuilder = None,
      httpBuilder = None,
      brief = Async[F].pure(None)
    )
}

object TaskGuard {

  def apply[F[_]: Async: Network](taskName: String): TaskGuard[F] =
    new TaskGuard[F](TaskConfig(taskName), Resource.pure(NoopEntrypoint[F]()))

  // for repl
  def dummyAgent[F[_]: Async: Network: Console]: Resource[F, GeneralAgent[F]] =
    apply("dummy").service("dummy").dummyAgent
}
