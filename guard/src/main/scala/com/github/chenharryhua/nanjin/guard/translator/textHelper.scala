package com.github.chenharryhua.nanjin.guard.translator

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionRetry, ServicePanic}
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot, NJEvent, ServiceStopCause}
import org.typelevel.cats.time.instances.{localdatetime, localtime}

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}

object textHelper extends localtime with localdatetime {
  def yamlMetrics(ss: MetricSnapshot): String =
    new SnapshotPolyglot(ss).toYaml

  def uptimeText(evt: NJEvent): String = fmt.format(evt.upTime)

  def tookText(dur: Duration): String         = fmt.format(dur)
  def tookText(dur: Option[Duration]): String = dur.map(fmt.format).getOrElse("Unknown")

  def hostText(sp: ServiceParams): String =
    sp.emberServerParams match {
      case Some(esp) => s"${sp.hostName.value}:${esp.port}"
      case None      => sp.hostName.value
    }

  def metricIndexText(index: MetricIndex): String =
    index match {
      case MetricIndex.Adhoc(_)       => "Adhoc"
      case MetricIndex.Periodic(tick) => show"${tick.index}"
    }

  def stopCause(ssc: ServiceStopCause): String = ssc match {
    case ServiceStopCause.Successfully       => "Successfully"
    case ServiceStopCause.ByCancellation     => "ByCancellation"
    case ServiceStopCause.ByException(error) => error.stack.mkString("\n\t")
    case ServiceStopCause.Maintenance        => "Maintenance"
  }

  def eventTitle(evt: NJEvent): String =
    evt match {
      case _: NJEvent.ActionStart => "Start Action"
      case _: NJEvent.ActionRetry => "Retry Action"
      case _: NJEvent.ActionFail  => "Action Failed"
      case _: NJEvent.ActionDone  => "Action Done"

      case NJEvent.ServiceAlert(_, _, _, _, al, _) => s"Alert ${al.productPrefix}"

      case NJEvent.ServiceStart(_, tick) =>
        if (tick.index === 0) "Start Service" else "Restart Service"
      case _: NJEvent.ServiceStop  => "Service Stopped"
      case _: NJEvent.ServicePanic => "Service Panic"

      case _: NJEvent.MetricReport => "Metric Report"
      case _: NJEvent.MetricReset  => "Metric Reset"
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
    val (time, dur) = localTime_duration(evt.timestamp, evt.tick.zonedWakeup)
    s"Restart was scheduled at $time, in $dur."
  }

  def retryText(evt: ActionRetry): String = {
    val (time, dur) = localTime_duration(evt.timestamp, evt.tick.zonedWakeup)
    val nth: String = to_ordinal_words(evt.tick.index)
    s"$nth retry was scheduled at $time, in $dur"
  }
}
