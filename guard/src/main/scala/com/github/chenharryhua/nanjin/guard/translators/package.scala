package com.github.chenharryhua.nanjin.guard

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{MetricReport, ServicePanic}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import org.apache.commons.lang3.StringUtils

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

package object translators {

  def nextTime(
    reportSchedule: Option[Either[FiniteDuration, CronExpr]],
    now: ZonedDateTime,
    interval: Option[FiniteDuration],
    launchTime: ZonedDateTime): Option[ZonedDateTime] = {
    val nextBorder: Option[ZonedDateTime] =
      interval.map(iv => launchTime.plus((((Duration.between(launchTime, now).toScala / iv).toLong + 1) * iv).toJava))
    nextBorder match {
      case None =>
        reportSchedule.flatMap {
          case Left(fd)  => Some(now.plus(fd.toJava))
          case Right(ce) => ce.next(now)
        }
      case Some(b) =>
        reportSchedule.flatMap {
          case Left(fd) =>
            LazyList.iterate(now)(_.plus(fd.toJava)).dropWhile(_.isBefore(b)).headOption
          case Right(ce) =>
            LazyList.unfold(now)(dt => ce.next(dt).map(t => Tuple2(t, t))).dropWhile(_.isBefore(b)).headOption
        }
    }
  }

  // slack not allow message larger than 3000 chars
  // https://api.slack.com/reference/surfaces/formatting
  final val MessageSizeLimits: Int = 2960

  private[translators] def abbreviate(msg: String): String = StringUtils.abbreviate(msg, MessageSizeLimits)

  private[translators] def hostServiceSection(sp: ServiceParams): JuxtaposeSection = {
    val sn: String = sp.taskParams.homePage.fold(sp.serviceName.value)(hp => s"<${hp.value}|${sp.serviceName.value}>")
    JuxtaposeSection(TextField("Service", sn), TextField("Host", sp.taskParams.hostName.value))
  }

  def toOrdinalWords(n: Long): String = {
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

  private[translators] val fmt: DurationFormatter = DurationFormatter.defaultFormatter

  private[translators] def localTimeAndDurationStr(start: ZonedDateTime, end: ZonedDateTime): (String, String) = {
    val duration = Duration.between(start, end)
    val localTime: String =
      if (duration.minus(Duration.ofHours(24)).isNegative)
        end.truncatedTo(ChronoUnit.SECONDS).toLocalTime.show
      else
        end.truncatedTo(ChronoUnit.SECONDS).toLocalDateTime.show

    (localTime, fmt.format(duration))
  }

  private[translators] def upcomingRestartTimeInterpretation(mr: MetricReport): String =
    mr.upcomingRestartTime match {
      case None => s"The service has been up and running for ${fmt.format(mr.upTime)}."
      case Some(zdt) =>
        val (time, dur) = localTimeAndDurationStr(mr.timestamp, zdt)
        s"The service is in panic, restart was scheduled at $time, in $dur."
    }

  private[translators] def upcomingRestartTimeInterpretation(sp: ServicePanic): String = {
    val upcoming: String = sp.upcomingRestartTime match {
      case None => "which should never happen." // never happen
      case Some(zdt) =>
        val (time, dur) = localTimeAndDurationStr(sp.timestamp, zdt)
        s"restart was scheduled at $time, in $dur."
    }
    s":alarm: The service experienced a panic, $upcoming"
  }
}
