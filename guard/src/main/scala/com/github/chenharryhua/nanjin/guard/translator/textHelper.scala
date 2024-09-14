package com.github.chenharryhua.nanjin.guard.translator

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.{MetricName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionRetry, ServicePanic}
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot, NJEvent, ServiceStopCause}
import org.typelevel.cats.time.instances.{localdatetime, localtime}

import java.time.temporal.ChronoUnit
import java.time.{Duration, LocalTime, ZonedDateTime}

object textHelper extends localtime with localdatetime {
  def yamlMetrics(ss: MetricSnapshot): String =
    new SnapshotPolyglot(ss).toYaml

  def uptimeText(evt: NJEvent): String = fmt.format(evt.upTime)

  def tookText(dur: Duration): String         = fmt.format(dur)
  def tookText(dur: Option[Duration]): String = dur.map(fmt.format).getOrElse("unknown")

  def hostText(sp: ServiceParams): String =
    sp.emberServerParams match {
      case Some(esp) => s"${sp.hostName.value}:${esp.port}"
      case None      => sp.hostName.value
    }

  def stopCause(ssc: ServiceStopCause): String = ssc match {
    case ServiceStopCause.Normally           => "Normally"
    case ServiceStopCause.ByCancellation     => "ByCancellation"
    case ServiceStopCause.ByException(error) => error.stack.mkString("\n\t")
    case ServiceStopCause.Maintenance        => "Maintenance"
  }

  def eventTitle(evt: NJEvent): String = {
    def name(mn: MetricName): String = s"[${mn.digest}][${mn.name}]"

    evt match {
      case NJEvent.ActionStart(_, ap, _, _)   => s"Start Action ${name(ap.metricName)}"
      case NJEvent.ActionRetry(_, ap, _, _)   => s"Action Retrying ${name(ap.metricName)}"
      case NJEvent.ActionFail(_, ap, _, _, _) => s"Action Failed ${name(ap.metricName)}"
      case NJEvent.ActionDone(_, ap, _, _, _) => s"Action Done ${name(ap.metricName)}"

      case NJEvent.ServiceAlert(metricName, _, _, al, _) =>
        s"Alert ${al.productPrefix} ${name(metricName)}"

      case NJEvent.ServiceStart(_, tick) =>
        if (tick.index === 0)
          "Start Service"
        else
          s"${to_ordinal_words(tick.index)} Restart Service"
      case _: NJEvent.ServiceStop  => "Service Stopped"
      case _: NJEvent.ServicePanic => "Service Panic"

      case NJEvent.MetricReport(index, _, _, _) =>
        index match {
          case MetricIndex.Adhoc(_)       => "Adhoc Metric Report"
          case MetricIndex.Periodic(tick) => s"Metric Report(index=${tick.index})"
        }
      case NJEvent.MetricReset(index, _, _, _) =>
        index match {
          case MetricIndex.Adhoc(_)       => "Adhoc Metric Reset"
          case MetricIndex.Periodic(tick) => s"Metric Reset(index=${tick.index})"
        }
    }
  }

  private def to_ordinal_words(n: Long): String = {
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

  private def localTime_duration(start: ZonedDateTime, end: ZonedDateTime): (String, String) = {
    val duration = Duration.between(start, end)
    val localTime: String =
      if (duration.minus(Duration.ofHours(24)).isNegative)
        end.truncatedTo(ChronoUnit.SECONDS).toLocalTime.show
      else
        end.truncatedTo(ChronoUnit.SECONDS).toLocalDateTime.show

    (localTime, fmt.format(duration))
  }

  def panicText(evt: ServicePanic): String = {
    val (time, dur) = localTime_duration(evt.timestamp, evt.restartTime)
    val nth: String = to_ordinal_words(evt.tick.index)
    s"$nth restart was scheduled at $time, roughly in $dur."
  }

  def retryText(evt: ActionRetry): String = {
    val localTs: LocalTime =
      evt.tick.zonedWakeup.toLocalTime.truncatedTo(ChronoUnit.SECONDS)
    val nth: String = to_ordinal_words(evt.tick.index)
    s"$nth retry was scheduled at $localTs"
  }
}
