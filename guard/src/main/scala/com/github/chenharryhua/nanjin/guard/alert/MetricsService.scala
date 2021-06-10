package com.github.chenharryhua.nanjin.guard.alert

import cats.effect.Sync
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ServiceParams}

import java.time.{Duration => JavaDuration}

final private class MetricsService[F[_]](metrics: MetricRegistry)(implicit F: Sync[F]) extends AlertService[F] {
  private def serviceKey(params: ServiceParams): String = s"${params.serviceName}.${params.taskParams.appName}"

  private def actionKey(info: ActionInfo, params: ActionParams): String =
    s"${info.actionName}.${params.serviceParams.serviceName}.${params.serviceParams.taskParams.appName}"

  override def alert(event: NJEvent): F[Unit] = event match {
    // counter
    case ServiceStarted(_, _, params) =>
      F.blocking(metrics.counter(serviceKey(params)).inc())
    case ServicePanic(_, _, params, _, _, _) =>
      F.blocking(metrics.counter(serviceKey(params)).inc())
    case ServiceStopped(_, _, params) =>
      F.blocking(metrics.counter(s"${serviceKey(params)}.stop").inc())
    case ServiceHealthCheck(_, _, params, _) =>
      F.blocking(metrics.counter(s"${serviceKey(params)}.health-check").inc())
    case ActionRetrying(_, info, params, _, _) =>
      F.blocking(metrics.counter(s"${actionKey(info, params)}.retry").inc())
    case _: ForYouInformation =>
      F.blocking(metrics.counter("fyi").inc())
    case PassThrough(_, _) =>
      F.blocking(metrics.counter("pass-through").inc())
    // timer
    case ActionFailed(at, info, params, _, _, _) =>
      F.blocking(metrics.timer(s"fail.${actionKey(info, params)}").update(JavaDuration.between(info.launchTime, at)))
    case ActionSucced(at, info, params, _, _) =>
      F.blocking(metrics.timer(s"succ.${actionKey(info, params)}").update(JavaDuration.between(info.launchTime, at)))
  }
}

object MetricsService {
  def apply[F[_]: Sync](metrics: MetricRegistry): AlertService[F] = new MetricsService[F](metrics)
}
