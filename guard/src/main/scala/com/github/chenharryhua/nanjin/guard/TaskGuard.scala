package com.github.chenharryhua.nanjin.guard
import cats.Endo
import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.{ServiceConfig, ServiceName, TaskName}
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import fs2.io.net.Network

/** poor man's telemetry
  */
final class TaskGuard[F[_]: Async: Network] private (serviceConfig: ServiceConfig[F])
    extends UpdateConfig[ServiceConfig[F], TaskGuard[F]] {

  override def updateConfig(f: Endo[ServiceConfig[F]]): TaskGuard[F] =
    new TaskGuard[F](f(serviceConfig))

  def service(serviceName: String): ServiceGuard[F] =
    new ServiceGuard[F](
      serviceName = ServiceName(serviceName),
      config = serviceConfig
    )
}

object TaskGuard {

  def apply[F[_]: Async: Network](taskName: String): TaskGuard[F] =
    new TaskGuard[F](ServiceConfig(TaskName(taskName)))

}
