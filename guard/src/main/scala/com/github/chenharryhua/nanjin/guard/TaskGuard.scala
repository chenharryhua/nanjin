package com.github.chenharryhua.nanjin.guard

import cats.ApplicativeError
import cats.data.NonEmptyList
import cats.effect.Sync
import cats.syntax.all._

/** credit to the excellent retry lib [[https://github.com/cb372/cats-retry]]
  */
final class TaskGuard[F[_]] private (
  applicationName: String,
  alertServices: NonEmptyList[AlertService[F]],
  serviceConfig: ServiceConfig,
  actionConfig: ActionConfig
) {

  def addAlertService(value: AlertService[F]): TaskGuard[F] =
    new TaskGuard[F](applicationName, value :: alertServices, serviceConfig, actionConfig)

  def fyi(msg: String)(implicit F: ApplicativeError[F, Throwable]): F[Unit] =
    alertServices.traverse(_.alert(ForYouInformation(applicationName, msg)).attempt).void

  def service(serviceName: String): ServiceGuard[F] =
    new ServiceGuard[F](applicationName, serviceName, alertServices, serviceConfig)

  def action(actionName: String): ActionGuard[F] =
    new ActionGuard[F](applicationName, actionName, alertServices, actionConfig)
}

object TaskGuard {

  def apply[F[_]: Sync](applicationName: String): TaskGuard[F] =
    new TaskGuard[F](applicationName, NonEmptyList.one(new LogService[F]), ServiceConfig.default, ActionConfig.default)
}
