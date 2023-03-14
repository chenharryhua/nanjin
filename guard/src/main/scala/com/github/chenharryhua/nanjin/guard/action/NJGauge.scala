package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Resource, Sync}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.MeasurementID
import com.github.chenharryhua.nanjin.guard.event.{MetricCategory, MetricID}
import io.circe.{Encoder, Json}
import io.circe.syntax.EncoderOps
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Instant

final class NJGauge[F[_]] private[guard] (
  id: MeasurementID,
  metricRegistry: MetricRegistry,
  dispatcher: Dispatcher[F])(implicit F: Sync[F]) {
  private val metricId: String = MetricID(id, MetricCategory.Gauge).asJson.noSpaces

  private def transErr(ex: Throwable): Json =
    Json.fromString(StringUtils.abbreviate(ExceptionUtils.getRootCauseMessage(ex), 80))

  private def elapse(start: Instant, end: Instant): Json =
    Json.fromString(DurationFormatter.defaultFormatter.format(start, end))

  def register[A: Encoder](value: F[A]): Resource[F, Unit] =
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
        metricId
      })(mId => F.delay(metricRegistry.remove(mId)).void)
      .void

  def register[A: Encoder](value: => A): Resource[F, Unit] = register(F.delay(value))

  val timed: Resource[F, Unit] =
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
        metricId
      })(mid => F.delay(metricRegistry.remove(mid)).void)
      .void
}
