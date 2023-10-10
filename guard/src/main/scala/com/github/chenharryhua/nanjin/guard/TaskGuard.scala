package com.github.chenharryhua.nanjin.guard
import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Console
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.{ServiceName, TaskConfig}
import com.github.chenharryhua.nanjin.guard.service.{GeneralAgent, ServiceGuard}
import fs2.io.net.Network

/** poor man's telemetry
  */
final class TaskGuard[F[_]: Async: Network] private (taskConfig: TaskConfig)
    extends UpdateConfig[TaskConfig, TaskGuard[F]] {

  override def updateConfig(f: Endo[TaskConfig]): TaskGuard[F] =
    new TaskGuard[F](f(taskConfig))

  def service(serviceName: String): ServiceGuard[F] =
    new ServiceGuard[F](
      serviceName = ServiceName(serviceName),
      taskParams = taskConfig.evalConfig,
      config = identity,
      httpBuilder = None,
      jmxBuilder = None,
      brief = Async[F].pure(None)
    )
}

object TaskGuard {

  def apply[F[_]: Async: Network](taskName: String): TaskGuard[F] =
    new TaskGuard[F](TaskConfig(taskName))

  // for repl
  def dummyAgent[F[_]: Async: Network: Console]: Resource[F, GeneralAgent[F]] =
    apply("dummy").service("dummy").dummyAgent
}
