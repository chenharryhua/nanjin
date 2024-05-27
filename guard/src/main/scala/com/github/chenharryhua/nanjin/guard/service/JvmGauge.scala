package com.github.chenharryhua.nanjin.guard.service

import cats.Eval
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.GaugeKind
import com.github.chenharryhua.nanjin.guard.config.{Category, MetricID, MetricName}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.util.Try

abstract class JvmGauge[F[_]] private[service] (metricRegistry: MetricRegistry)(implicit F: Sync[F]) {

  private val measurement: String = "jvm"

  private def json_gauge[A: Encoder](metricID: MetricID, data: Eval[A]): Resource[F, Unit] = {
    def trans_error(ex: Throwable): Json =
      Json.fromString(StringUtils.abbreviate(ExceptionUtils.getRootCauseMessage(ex), 80))

    Resource
      .make(F.delay {
        metricRegistry.gauge(
          metricID.identifier,
          () =>
            new Gauge[Json] {
              override def getValue: Json =
                Try(data.value).fold(trans_error, _.asJson)
            }
        )
      })(_ => F.delay(metricRegistry.remove(metricID.identifier)).void)
      .void
  }

  val classloader: Resource[F, Unit] = {
    val name: MetricName = MetricName("classloader", "00001", measurement)
    Resource.eval(F.unique).flatMap { token =>
      val metricID: MetricID = MetricID(name, Category.Gauge(GaugeKind.Instrument), token.hash)

      json_gauge(metricID, mxBeans.classloader)
    }
  }

  val deadlocks: Resource[F, Unit] = {
    val name: MetricName = MetricName("deadlocks", "00002", measurement)
    Resource.eval(F.unique).flatMap { token =>
      val metricID: MetricID = MetricID(name, Category.Gauge(GaugeKind.Deadlocks), token.hash)

      json_gauge(metricID, mxBeans.deadlocks)
    }
  }

  val garbageCollectors: Resource[F, Unit] = {
    val name: MetricName = MetricName("garbage_collectors", "00003", measurement)
    Resource.eval(F.unique).flatMap { token =>
      val metricID: MetricID = MetricID(name, Category.Gauge(GaugeKind.Instrument), token.hash)

      json_gauge(metricID, mxBeans.garbageCollectors)
    }
  }

  val heapMemory: Resource[F, Unit] = {
    val name: MetricName = MetricName("heap_memory", "00004", measurement)
    Resource.eval(F.unique).flatMap { token =>
      val metricID: MetricID = MetricID(name, Category.Gauge(GaugeKind.Instrument), token.hash)

      json_gauge(metricID, mxBeans.heapMemory)
    }
  }

  val nonHeapMemory: Resource[F, Unit] = {
    val name: MetricName = MetricName("non_heap_memory", "00005", measurement)
    Resource.eval(F.unique).flatMap { token =>
      val metricID: MetricID = MetricID(name, Category.Gauge(GaugeKind.Instrument), token.hash)

      json_gauge(metricID, mxBeans.nonHeapMemory)
    }
  }

  val threadState: Resource[F, Unit] = {
    val name: MetricName = MetricName("thread_state", "00006", measurement)
    Resource.eval(F.unique).flatMap { token =>
      val metricID: MetricID = MetricID(name, Category.Gauge(GaugeKind.Instrument), token.hash)

      json_gauge(metricID, mxBeans.threadState)
    }
  }
}
