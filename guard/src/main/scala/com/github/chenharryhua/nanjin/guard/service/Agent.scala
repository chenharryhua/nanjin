package com.github.chenharryhua.nanjin.guard.service

import cats.{Alternative, Endo, Traverse}
import cats.data.{Ior, IorT}
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
  entryPoint: EntryPoint[F])(implicit F: Async[F])
    extends EntryPoint[F] {

  override def root(name: String): Resource[F, NJSpan[F]] =
    entryPoint.root(name).map(s => new NJSpan[F](name, s))

  override def continue(name: String, kernel: Kernel): Resource[F, NJSpan[F]] =
    entryPoint.continue(name, kernel).map(s => new NJSpan[F](name, s))

  override def continueOrElseRoot(name: String, kernel: Kernel): Resource[F, NJSpan[F]] =
    entryPoint.continueOrElseRoot(name, kernel).map(s => new NJSpan[F](name, s))

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

  def quasi[G[_]: Traverse: Alternative, B](tfb: G[F[B]]): IorT[F, G[Throwable], G[B]] =
    IorT(tfb.traverse(_.attempt).map(_.partitionEither(identity)).map { case (fail, succ) =>
      (fail.size, succ.size) match {
        case (0, _) => Ior.Right(succ)
        case (_, 0) => Ior.left(fail)
        case _      => Ior.Both(fail, succ)
      }
    })

  def quasi[B](fbs: F[B]*): IorT[F, List[Throwable], List[B]] = quasi[List, B](fbs.toList)

  def quasi[G[_]: Traverse: Alternative, B](parallelism: Int, tfb: G[F[B]]): IorT[F, G[Throwable], G[B]] =
    IorT(
      F.parTraverseN(parallelism)(tfb)(_.attempt).map(_.partitionEither(identity)).map { case (fail, succ) =>
        (fail.size, succ.size) match {
          case (0, _) => Ior.Right(succ)
          case (_, 0) => Ior.left(fail)
          case _      => Ior.Both(fail, succ)
        }
      })

  def quasi[B](parallelism: Int)(tfb: F[B]*): IorT[F, List[Throwable], List[B]] =
    quasi[List, B](parallelism, tfb.toList)

}
