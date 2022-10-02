package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Ref}
import cats.effect.Resource
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import fs2.Stream
import natchez.{EntryPoint, Kernel}

import java.time.ZoneId

final class Agent[F[_]] private[service] (
  metricRegistry: MetricRegistry,
  serviceStatus: Ref[F, ServiceStatus],
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  entryPoint: Resource[F, EntryPoint[F]])(implicit F: Async[F])
    extends EntryPoint[F] {

  override def root(name: String): Resource[F, NJSpan[F]] =
    entryPoint.flatMap(_.root(name).map(s => new NJSpan[F](name, s)))

  override def continue(name: String, kernel: Kernel): Resource[F, NJSpan[F]] =
    entryPoint.flatMap(_.continue(name, kernel).map(s => new NJSpan[F](name, s)))

  override def continueOrElseRoot(name: String, kernel: Kernel): Resource[F, NJSpan[F]] =
    entryPoint.flatMap(_.continueOrElseRoot(name, kernel).map(s => new NJSpan[F](name, s)))

  val zoneId: ZoneId = serviceParams.taskParams.zoneId

  def action(cfg: Endo[ActionConfig]): NJActionBuilder[F] =
    new NJActionBuilder[F](
      metricRegistry = metricRegistry,
      channel = channel,
      actionConfig = cfg(ActionConfig(serviceParams)))

  def broker(brokerName: String): NJBroker[F] =
    new NJBroker[F](
      digested = Digested(serviceParams, brokerName),
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = serviceParams,
      isError = false,
      isCounting = false
    )

  def alert(alertName: String): NJAlert[F] =
    new NJAlert(
      digested = Digested(serviceParams, alertName),
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = serviceParams,
      isCounting = false
    )

  def counter(counterName: String): NJCounter[F] =
    new NJCounter(
      digested = Digested(serviceParams, counterName),
      metricRegistry = metricRegistry,
      isError = false)

  def meter(meterName: String): NJMeter[F] =
    new NJMeter[F](
      digested = Digested(serviceParams, meterName),
      metricRegistry = metricRegistry,
      isCounting = false)

  def histogram(histoName: String): NJHistogram[F] =
    new NJHistogram[F](
      digested = Digested(serviceParams, histoName),
      metricRegistry = metricRegistry,
      isCounting = false
    )

  lazy val metrics: NJMetrics[F] =
    new NJMetrics[F](channel = channel, metricRegistry = metricRegistry, serviceStatus = serviceStatus)

  lazy val runtime: NJRuntimeInfo[F] = new NJRuntimeInfo[F](serviceStatus = serviceStatus)

  // for convenience

  def nonStop[A](sfa: Stream[F, A]): F[Nothing] =
    action(_.withoutTiming.withoutCounting.trivial.withAlwaysGiveUp)
      .retry(sfa.compile.drain)
      .run("nonStop")
      .flatMap[Nothing](_ => F.raiseError(ActionException.UnexpectedlyTerminated))
}
