package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

final class ServiceGuard[F[_]](alertServices: List[AlertService[F]], config: ServiceConfig) {
  val params: ServiceParams = config.evalConfig

  def updateConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](alertServices, f(config))

  private def healthCheck(implicit F: Async[F]): F[Unit] =
    alertServices
      .traverse(
        _.alert(
          ServiceHealthCheck(
            applicationName = params.applicationName,
            serviceName = params.serviceName,
            healthCheckInterval = params.healthCheckInterval)))
      .delayBy(params.healthCheckInterval)
      .void
      .foreverM[Unit]

  private def start(implicit F: Async[F]): F[Unit] =
    alertServices
      .traverse(_.alert(ServiceStarted(applicationName = params.applicationName, serviceName = params.serviceName)))
      .void
      .delayBy(params.retryPolicy.value)

  private def abnormalStop(implicit F: Async[F]): F[Unit] =
    alertServices
      .traverse(
        _.alert(
          ServiceAbnormalStop(
            applicationName = params.applicationName,
            serviceName = params.serviceName,
            error = new Exception(s"service(${params.serviceName}) was abnormally stopped.")
          )))
      .delayBy(params.retryPolicy.value)
      .void
      .foreverM[Unit]

  def run[A](action: F[A])(implicit F: Async[F]): F[Unit] = {

    def onError(err: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case wd @ WillDelayAndRetry(_, _, _) =>
          alertServices
            .traverse(
              _.alert(
                ServiceRestarting(
                  applicationName = params.applicationName,
                  serviceName = params.serviceName,
                  willDelayAndRetry = wd,
                  error = err)))
            .void
        case GivingUp(_, _) =>
          alertServices
            .traverse(
              _.alert(
                ServiceAbnormalStop(
                  applicationName = params.applicationName,
                  serviceName = params.serviceName,
                  error = err)))
            .void
      }

    retry.retryingOnAllErrors(params.retryPolicy.policy[F], onError)(
      F.bracket((start *> healthCheck).start)(_ => action)(_.cancel).flatMap(_ => abnormalStop))
  }

  def run[A](stream: Stream[F, A])(implicit F: Async[F]): Stream[F, Unit] =
    Stream.eval(run(stream.compile.drain))

}
