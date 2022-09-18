package com.github.chenharryhua.nanjin.guard.service

import cats.{Alternative, Traverse}
import cats.data.{Ior, IorT}
import cats.effect.kernel.{Async, Ref}
import cats.effect.Resource
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import natchez.{EntryPoint, Kernel, Span}

import java.time.ZoneId

final class Agent[F[_]] private[service] (
  entryPoint: Resource[F, EntryPoint[F]],
  metricRegistry: MetricRegistry,
  serviceStatus: Ref[F, ServiceStatus],
  channel: Channel[F, NJEvent],
  agentConfig: AgentConfig)(implicit F: Async[F])
    extends EntryPoint[F] {

  private def build(span: Span[F], name: String) = new NJSpan[F](
    nativeSpan = span,
    spanName = name,
    metricRegistry = metricRegistry,
    serviceStatus = serviceStatus,
    channel = channel,
    agentConfig = agentConfig)

  override def root(name: String): Resource[F, NJSpan[F]] =
    entryPoint.flatMap(_.root(name).map(build(_, name)))
  override def continue(name: String, kernel: Kernel): Resource[F, NJSpan[F]] =
    entryPoint.flatMap(_.continue(name, kernel).map(build(_, name)))
  override def continueOrElseRoot(name: String, kernel: Kernel): Resource[F, NJSpan[F]] =
    entryPoint.flatMap(_.continueOrElseRoot(name, kernel).map(build(_, name)))

  // parameters

  private lazy val agentParams: AgentParams     = agentConfig.evalConfig
  private lazy val serviceParams: ServiceParams = agentParams.serviceParams

  lazy val zoneId: ZoneId = agentParams.serviceParams.taskParams.zoneId

  def broker(brokerName: String): NJBroker[F] =
    new NJBroker[F](
      digested = Digested(serviceParams, brokerName),
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = agentParams.serviceParams,
      isError = false,
      isCounting = false
    )

  def alert(alertName: String): NJAlert[F] =
    new NJAlert(
      digested = Digested(serviceParams, alertName),
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = agentParams.serviceParams,
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
    new NJMetrics[F](
      new MetricEventPublisher[F](
        channel = channel,
        metricRegistry = metricRegistry,
        serviceStatus = serviceStatus))

  lazy val runtime: NJRuntimeInfo[F] = new NJRuntimeInfo[F](serviceStatus = serviceStatus)

  // for convenience

//  def nonStop[B](fb: F[B]): F[Nothing] =
//    action("non-stop")
//      .updateConfig(_.withoutTiming.withoutCounting.withLowImportance.withExpensive(true).withAlwaysGiveUp)
//      .retry(fb)
//      .run
//      .flatMap[Nothing](_ => F.raiseError(ActionException.UnexpectedlyTerminated))

//  def nonStop[B](sfb: Stream[F, B]): F[Nothing] = nonStop(sfb.compile.drain)

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
