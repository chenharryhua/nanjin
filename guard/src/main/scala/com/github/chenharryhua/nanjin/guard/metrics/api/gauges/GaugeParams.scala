package com.github.chenharryhua.nanjin.guard.metrics.api.gauges

import cats.effect.std.Dispatcher
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.metrics.MetricLabel

import java.time.ZoneId

final private[metrics] case class GaugeParams[F[_]](
  dispatcher: Dispatcher[F],
  metricRegistry: MetricRegistry,
  label: MetricLabel,
  zoneId: ZoneId)
