package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.{Async, Resource}
import cats.syntax.semigroup.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.{HostName, UpdateConfig}
import com.github.chenharryhua.nanjin.guard.alert.{AlertService, NJMetricsReporter}
import com.github.chenharryhua.nanjin.guard.config.{ServiceConfig, TaskConfig, TaskParams}

/** credit to the excellent retry lib [[https://github.com/cb372/cats-retry]]
  */
final class TaskGuard[F[_]: Async] private (
  taskConfig: TaskConfig,
  alertServices: Resource[F, AlertService[F]],
  reporters: List[NJMetricsReporter])
    extends UpdateConfig[TaskConfig, TaskGuard[F]] with HasAlertService[F, TaskGuard[F]] {
  val params: TaskParams = taskConfig.evalConfig

  override def updateConfig(f: TaskConfig => TaskConfig): TaskGuard[F] =
    new TaskGuard[F](f(taskConfig), alertServices, reporters)

  def service(serviceName: String): ServiceGuard[F] =
    new ServiceGuard[F](ServiceConfig(serviceName, params), alertServices, reporters)

  override def addAlertService(ras: Resource[F, AlertService[F]]): TaskGuard[F] =
    new TaskGuard[F](taskConfig, alertServices.flatMap(as => ras.map(_ |+| as)), reporters)

  def addReporter(reporter: NJMetricsReporter): TaskGuard[F] =
    new TaskGuard[F](taskConfig, alertServices, reporter :: reporters)
}

object TaskGuard {

  def apply[F[_]: Async](applicationName: String): TaskGuard[F] =
    new TaskGuard[F](
      TaskConfig(applicationName, HostName.local_host),
      Resource.pure(AlertService.monoidAlertService.empty),
      Nil)
}
