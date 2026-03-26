package com.github.chenharryhua.nanjin.guard

import cats.Eval
import cats.syntax.eq.catsSyntaxEq
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.ServicePanic
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Kind.{Report, Reset}
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

  final val decimalFormatter: DecimalFormat = new DecimalFormat("#,###")

  def eventTitle(evt: Event): String =
    evt match {
      case ss: Event.ServiceStart =>
        if (ss.tick.index === 0) "Start Service" else "Restart Service"
      case _: Event.ServiceStop      => "Stop Service"
      case _: Event.ServicePanic     => "Service Panic"
      case _: Event.ReportedEvent    => "Reported Event"
      case ms: Event.MetricsSnapshot =>
        ms.kind match {
          case Report(_) => "Metrics Report"
          case Reset(_)  => "Metrics Reset"
        }
    }

  private def localTime_duration(start: ZonedDateTime, end: ZonedDateTime): (String, String) = {
    val duration = Duration.between(start, end)
    val localTime: String =
      if (duration.minus(Duration.ofHours(24)).isNegative)
        end.truncatedTo(ChronoUnit.SECONDS).toLocalTime.show
      else
        end.truncatedTo(ChronoUnit.SECONDS).toLocalDateTime.show

    (localTime, defaultFormatter.format(duration))
  }

  def panicText(evt: ServicePanic): String = {
    val (time, dur) = localTime_duration(evt.timestamp.value, evt.tick.zoned(_.conclude))
    s"Restart scheduled for $time, in $dur."
  }

  def interpretServiceParams(serviceParams: ServiceParams): Json =
    Json.obj(
      Attribute(serviceParams.taskName).snakeJsonEntry,
      Attribute(serviceParams.serviceName).snakeJsonEntry,
      Attribute(serviceParams.serviceId).snakeJsonEntry,
      Attribute(serviceParams.homepage).snakeJsonEntry,
      Attribute(serviceParams.host).map(_.show).snakeJsonEntry,
      "service_policies" -> Json.obj(
        "restart" -> Json.obj(
          Attribute(serviceParams.servicePolicies.restart.policy).map(_.show).snakeJsonEntry,
          "threshold" -> serviceParams.servicePolicies.restart.threshold.map(defaultFormatter.format).asJson
        ),

        "metrics_realtime" ->
          serviceParams.servicePolicies.realtimeMetrics.map { tm =>
            Json.obj(
              Attribute(tm.policy).map(_.show).snakeJsonEntry,
              "max_points" -> Json.fromInt(tm.maxPoints)
            )
          }.asJson,
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

  def htmlColoring(evt: Event): String = ColorScheme
    .decorate[Eval, String](evt)
    .run {
      case ColorScheme.GoodColor  => Eval.now("color:darkgreen")
      case ColorScheme.InfoColor  => Eval.now("color:black")
      case ColorScheme.WarnColor  => Eval.now("color:#b3b300")
      case ColorScheme.ErrorColor => Eval.now("color:red")
      case ColorScheme.DebugColor => Eval.now("color:#FF00FF")
    }
    .value
}
