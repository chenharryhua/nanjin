package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Ref, Resource, Sync}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.{Category, GaugeKind, MetricID, MetricName}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Instant

final class NJGauge[F[_]] private[guard] (
  name: MetricName,
  metricRegistry: MetricRegistry,
  dispatcher: Dispatcher[F])(implicit F: Sync[F]) {
  private def transErr(ex: Throwable): Json =
    Json.fromString(StringUtils.abbreviate(ExceptionUtils.getRootCauseMessage(ex), 80))

  private def elapse(start: Instant, end: Instant): Json =
    Json.fromString(DurationFormatter.defaultFormatter.format(start, end))

  def register[A: Encoder](value: F[A]): Resource[F, Unit] = {
    val metricId: String = MetricID(name, Category.Gauge(GaugeKind.Dropwizard)).asJson.noSpaces
    Resource
      .make(F.delay {
        metricRegistry.gauge(
          metricId,
          () =>
            new Gauge[String] {
              override def getValue: String =
                dispatcher.unsafeRunSync(value.attempt).fold(transErr(_).noSpaces, _.asJson.noSpaces)
            }
        )
      })(_ => F.delay(metricRegistry.remove(metricId)).void)
      .void
  }

  def register[A: Encoder](value: => A): Resource[F, Unit] = register(F.delay(value))

  val timed: Resource[F, Unit] = {
    val metricId: String = MetricID(name, Category.Gauge(GaugeKind.TimedGauge)).asJson.noSpaces
    Resource
      .make(F.realTimeInstant.map { kickoff =>
        metricRegistry.gauge(
          metricId,
          () =>
            new Gauge[String] {
              override def getValue: String =
                dispatcher.unsafeRunSync(F.realTimeInstant.map(elapse(kickoff, _).noSpaces))
            }
        )
      })(_ => F.delay(metricRegistry.remove(metricId)).void)
      .void
  }

  def ref[A: Encoder](value: F[Ref[F, A]]): Resource[F, Ref[F, A]] = {
    val metricId: String = MetricID(name, Category.Gauge(GaugeKind.RefGauge)).asJson.noSpaces
    Resource.make(value.map { ref =>
      metricRegistry.gauge(
        metricId,
        () =>
          new Gauge[String] {
            override def getValue: String = dispatcher.unsafeRunSync(ref.get).asJson.noSpaces
          }
      )
      ref
    })(_ => F.delay(metricRegistry.remove(metricId)).void)
  }
}
