package com.github.chenharryhua.nanjin.guard

import cats.syntax.eq.catsSyntaxEq
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.ServicePanic
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.apache.commons.lang3.StringUtils
import org.typelevel.cats.time.instances.localdatetime.localdatetimeInstances
import org.typelevel.cats.time.instances.localtime.localtimeInstances

import java.text.DecimalFormat
import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}

package object translator {
  private[translator] val space2: String = StringUtils.SPACE * 2
  private[translator] val space4: String = StringUtils.SPACE * 4

  final val durationFormatter: DurationFormatter = DurationFormatter.defaultFormatter
  final val decimalFormatter: DecimalFormat = new DecimalFormat("#,###")

  def eventTitle(evt: Event): String =
    evt match {
      case ss: Event.ServiceStart =>
        if (ss.tick.index === 0) "Start Service" else "Restart Service"
      case _: Event.ServiceStop    => "Service Stopped"
      case _: Event.ServicePanic   => "Service Panic"
      case _: Event.ServiceMessage => "Service Message"
      case _: Event.MetricsReport  => "Metrics Report"
      case _: Event.MetricsReset   => "Metrics Reset"
    }

  private def localTime_duration(start: ZonedDateTime, end: ZonedDateTime): (String, String) = {
    val duration = Duration.between(start, end)
    val localTime: String =
      if (duration.minus(Duration.ofHours(24)).isNegative)
        end.truncatedTo(ChronoUnit.SECONDS).toLocalTime.show
      else
        end.truncatedTo(ChronoUnit.SECONDS).toLocalDateTime.show

    (localTime, durationFormatter.format(duration))
  }

  def panicText(evt: ServicePanic): String = {
    val (time, dur) = localTime_duration(evt.timestamp.value, evt.tick.zoned(_.conclude))
    s"Restart was scheduled at $time, in $dur."
  }

  def interpretServiceParams(serviceParams: ServiceParams): Json =
    Json.obj(
      Attribute(serviceParams.taskName).snakeJsonEntry,
      Attribute(serviceParams.serviceName).snakeJsonEntry,
      Attribute(serviceParams.serviceId).snakeJsonEntry,
      Attribute(serviceParams.homepage).snakeJsonEntry,
      Attribute(serviceParams.host).snakeJsonEntry,
      "service_policies" -> Json.obj(
        "restart" -> Json.obj(
          Attribute(serviceParams.servicePolicies.restart.policy).snakeJsonEntry(_.show.asJson),
          "threshold" -> serviceParams.servicePolicies.restart.threshold.map(defaultFormatter.format).asJson
        ),
        "metrics_report" -> serviceParams.servicePolicies.metricsReport.show.asJson,
        "metrics_reset" -> serviceParams.servicePolicies.metricsReset.show.asJson
      ),
      "launch_time" -> serviceParams.launchTime.asJson,
      Attribute(serviceParams.logFormat).snakeJsonEntry,
      "history_capacity" -> Json.obj(
        "metrics_queue_size" -> serviceParams.historyCapacity.metric.asJson,
        "error_queue_size" -> serviceParams.historyCapacity.error.asJson,
        "panic_queue_size" -> serviceParams.historyCapacity.panic.asJson
      ),
      "nanjin" -> serviceParams.nanjin.asJson,
      Attribute(serviceParams.brief).snakeJsonEntry
    )

}
