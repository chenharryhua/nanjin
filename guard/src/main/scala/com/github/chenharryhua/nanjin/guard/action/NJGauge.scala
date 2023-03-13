package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Resource, Sync}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.Digested
import com.github.chenharryhua.nanjin.guard.event.{MetricCategory, MetricID, NJError}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

final class NJGauge[F[_]] private[guard] (
  digested: Digested,
  metricRegistry: MetricRegistry,
  dispatcher: Dispatcher[F])(implicit F: Sync[F]) {
  private val metricId = MetricID(digested, MetricCategory.Gauge).asJson.noSpaces

  def register[A: Encoder](value: F[A]): Resource[F, Unit] =
    Resource
      .make(F.delay {
        metricRegistry.gauge(
          metricId,
          () =>
            new Gauge[String] {
              override def getValue: String =
                dispatcher
                  .unsafeRunSync(value.attempt)
                  .fold(ex => NJError(ex).asJson.noSpaces, _.asJson.noSpaces)
            }
        )
        metricId
      })(mId => F.delay(metricRegistry.remove(mId)).void)
      .void

  def register[A: Encoder](value: => A): Resource[F, Unit] = register(F.delay(value))
}
