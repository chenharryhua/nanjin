package com.github.chenharryhua.nanjin.guard.action

import cats.Show
import cats.syntax.show.*
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.Digested
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.util.Try

final class NJGauge private[guard] (digested: Digested, metricRegistry: MetricRegistry) {

  def register[A: Show](value: => A): Gauge[String] =
    metricRegistry.gauge(
      gaugeMRName(digested),
      () =>
        new Gauge[String] {
          override def getValue: String = Try(value).fold(ExceptionUtils.getMessage, _.show)
        }
    )
}
