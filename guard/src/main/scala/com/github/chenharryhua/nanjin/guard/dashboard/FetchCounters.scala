package com.github.chenharryhua.nanjin.guard.dashboard

import cats.Eval
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.event.MetricID
import io.circe.jawn.decode

import scala.jdk.CollectionConverters.MapHasAsScala

final private class FetchCounters(metricRegistry: MetricRegistry) {

  val meters: Eval[Map[MetricID, Long]] =
    Eval.always {
      metricRegistry.getMeters().asScala
        .foldLeft(Map.empty[MetricID, Long]) { case (map, (name, meter)) =>
          decode[MetricID](name) match {
            case Left(_)    => map
            case Right(mid) => map + (mid -> meter.getCount)
          }
        }
    }

  val timers: Eval[Map[MetricID, Long]] =
    Eval.always {
      metricRegistry.getTimers().asScala
        .foldLeft(Map.empty[MetricID, Long]) { case (map, (name, timer)) =>
          decode[MetricID](name) match {
            case Left(_)    => map
            case Right(mid) => map + (mid -> timer.getCount)
          }
        }
    }
}
