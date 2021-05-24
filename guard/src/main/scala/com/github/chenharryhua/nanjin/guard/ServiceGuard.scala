package com.github.chenharryhua.nanjin.guard

import cats.data.NonEmptyList
import cats.effect.syntax.all._
import cats.effect.{Async, Sync}
import cats.syntax.all._
import fs2.Stream
import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.Instant

final class ServiceGuard[F[_]](
  applicationName: String,
  serviceName: String,
  alertServices: NonEmptyList[AlertService[F]],
  config: ServiceConfig) {
  val params: ServiceParams = config.evalConfig

  def updateConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](applicationName, serviceName, alertServices, f(config))

  def run[A](action: F[A])(implicit F: Async[F]): F[Unit] = {
    val serviceInfo: ServiceInfo =
      ServiceInfo(serviceName, params.healthCheckInterval, params.retryPolicy.policy[F].show, Instant.now())
    retry.retryingOnAllErrors(params.retryPolicy.policy[F], onError(serviceInfo)) {
      val register: F[Unit] = startUp(serviceInfo) >> healthCheck(serviceInfo)
      register.start.bracket(_ => action)(_.cancel) >> abnormalStop(serviceInfo)
    }
  }

  def run[A](stream: Stream[F, A])(implicit F: Async[F]): Stream[F, Unit] =
    Stream.eval(run(stream.compile.drain))

  private def healthCheck(serviceInfo: ServiceInfo)(implicit F: Async[F]): F[Unit] =
    alertServices
      .traverse(_.alert(ServiceHealthCheck(applicationName = applicationName, serviceInfo = serviceInfo)).attempt)
      .delayBy(params.healthCheckInterval)
      .void
      .foreverM

  private def startUp(serviceInfo: ServiceInfo)(implicit F: Async[F]): F[Unit] =
    alertServices
      .traverse(_.alert(ServiceStarted(applicationName = applicationName, serviceInfo = serviceInfo)).attempt)
      .delayBy(params.retryPolicy.value)
      .void

  private def abnormalStop(serviceInfo: ServiceInfo)(implicit F: Async[F]): F[Unit] =
    alertServices
      .traverse(_.alert(ServiceAbnormalStop(applicationName = applicationName, serviceInfo = serviceInfo)).attempt)
      .void

  private def onError(serviceInfo: ServiceInfo)(err: Throwable, details: RetryDetails)(implicit F: Sync[F]): F[Unit] =
    details match {
      case wd @ WillDelayAndRetry(_, _, _) =>
        alertServices
          .traverse(
            _.alert(
              ServicePanic(
                applicationName = applicationName,
                serviceInfo = serviceInfo,
                willDelayAndRetry = wd,
                error = err
              )).attempt)
          .void
      case GivingUp(_, _) => F.unit // never happen
    }
}
