package com.github.chenharryhua.nanjin.guard.alert

import cats.effect.Sync
import com.codahale.metrics.MetricRegistry

import java.time.{Duration => JavaDuration}

final private class MetricsService[F[_]](metrics: MetricRegistry)(implicit F: Sync[F]) extends AlertService[F] {

  override def alert(event: NJEvent): F[Unit] = event match {
    // counter
    case ServiceStarted(serviceInfo) =>
      F.blocking(metrics.counter(s"${serviceInfo.metricsKey}.start").inc())
    case ServicePanic(serviceInfo, _, _, _) =>
      F.blocking(metrics.counter(s"${serviceInfo.metricsKey}.panic").inc())
    case ServiceStopped(serviceInfo) =>
      F.blocking(metrics.counter(s"${serviceInfo.metricsKey}.stop").inc())
    case ServiceHealthCheck(serviceInfo, _) =>
      F.blocking(metrics.counter(s"${serviceInfo.metricsKey}.health-check").inc())
    case ActionRetrying(actionInfo, _, _) =>
      F.blocking(metrics.counter(s"${actionInfo.metricsKey}.retry").inc())
    case _: ForYouInformation =>
      F.blocking(metrics.counter("fyi").inc())
    // timer
    case ActionFailed(actionInfo, _, endAt, _, _) =>
      F.blocking(
        metrics.timer(s"fail.${actionInfo.metricsKey}").update(JavaDuration.between(actionInfo.launchTime, endAt)))
    case ActionSucced(actionInfo, endAt, _, _) =>
      F.blocking(
        metrics.timer(s"succ.${actionInfo.metricsKey}").update(JavaDuration.between(actionInfo.launchTime, endAt)))

  }
}

object MetricsService {
  def apply[F[_]: Sync](metrics: MetricRegistry): AlertService[F] = new MetricsService[F](metrics)
}
