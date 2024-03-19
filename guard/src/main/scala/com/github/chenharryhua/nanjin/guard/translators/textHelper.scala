package com.github.chenharryhua.nanjin.guard.translators

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.MetricName
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionResultEvent, ActionRetry, ServicePanic}
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot, NJEvent}
import org.typelevel.cats.time.instances.{localdatetime, localtime}

import java.time.temporal.ChronoUnit
import java.time.{Duration, LocalTime, ZonedDateTime}

private object textHelper extends localtime with localdatetime {
  def yamlMetrics(ss: MetricSnapshot): String =
    new SnapshotPolyglot(ss).toYaml

  def upTimeText(evt: NJEvent): String         = fmt.format(evt.upTime)
  def tookText(evt: ActionResultEvent): String = evt.took.map(fmt.format).getOrElse("unknown")

  def eventTitle(evt: NJEvent): String = {
    def name(mn: MetricName): String = s"[${mn.digest}][${mn.name}]"

    evt match {
      case NJEvent.ActionStart(ap, _, _, _)      => s"Start Action ${name(ap.metricName)}"
      case NJEvent.ActionRetry(ap, _, _, _, _)   => s"Action Retrying ${name(ap.metricName)}"
      case NJEvent.ActionFail(ap, _, _, _, _, _) => s"Action Failed ${name(ap.metricName)}"
      case NJEvent.ActionDone(ap, _, _, _, _)    => s"Action Done ${name(ap.metricName)}"

      case NJEvent.ServiceAlert(metricName, _, _, al, _) =>
        s"Alert ${al.productPrefix} ${name(metricName)}"

      case NJEvent.ServiceStart(_, tick) => if (tick.index === 0) "Start Service" else "Restart Service"
      case _: NJEvent.ServiceStop        => "Service Stopped"
      case _: NJEvent.ServicePanic       => "Service Panic"

      case NJEvent.MetricReport(index, _, _, _) =>
        index match {
          case MetricIndex.Adhoc          => "Adhoc Metric Report"
          case MetricIndex.Periodic(tick) => s"Metric Report(index=${tick.index})"
        }
      case NJEvent.MetricReset(index, _, _, _) =>
        index match {
          case MetricIndex.Adhoc          => "Adhoc Metric Reset"
          case MetricIndex.Periodic(tick) => s"Metric Reset(index=${tick.index})"
        }
    }
  }

  private def toOrdinalWords(n: Long): String = {
    val w =
      if (n % 100 / 10 == 1) "th"
      else {
        n % 10 match {
          case 1 => "st"
          case 2 => "nd"
          case 3 => "rd"
          case _ => "th"
        }
      }
    s"$n$w"
  }

  private def localTimeAndDurationStr(start: ZonedDateTime, end: ZonedDateTime): (String, String) = {
    val duration = Duration.between(start, end)
    val localTime: String =
      if (duration.minus(Duration.ofHours(24)).isNegative)
        end.truncatedTo(ChronoUnit.SECONDS).toLocalTime.show
      else
        end.truncatedTo(ChronoUnit.SECONDS).toLocalDateTime.show

    (localTime, fmt.format(duration))
  }

  def panicText(evt: ServicePanic): String = {
    val (time, dur) = localTimeAndDurationStr(evt.timestamp, evt.restartTime)
    s"The service experienced a panic. Restart was scheduled at $time, roughly in $dur."
  }

  def retryText(evt: ActionRetry): String = {
    val localTs: LocalTime =
      evt.tick.zonedWakeup.toLocalTime.truncatedTo(ChronoUnit.SECONDS)
    s"${toOrdinalWords(evt.tick.index)} retry was scheduled at $localTs"
  }
}
