package com.github.chenharryhua.nanjin.guard

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.ServiceStatus
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import org.apache.commons.lang3.StringUtils

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZoneId, ZonedDateTime}
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

  private[guard] def hostServiceSection(sp: ServiceParams): JuxtaposeSection = {
    val sn = sp.taskParams.homePage.fold(sp.metricName.metricRepr)(hp => s"<$hp|${sp.metricName.origin}>")
    JuxtaposeSection(TextField("Service", sn), TextField("Host", sp.taskParams.hostName))
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

  private[translators] def localTimestampStr(zdt: ZonedDateTime): String =
    zdt.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show

  private[translators] def serviceStatusWord(ss: ServiceStatus, zoneId: ZoneId): String =
    ss.fold(
      _ => "Service is Up",
      down =>
        down.upcommingRestart match {
          case Some(ts) =>
            s"${down.cause} occured at ${localTimestampStr(
              down.crashAt.atZone(zoneId))}. restart is scheduled at ${localTimestampStr(ts.atZone(zoneId))}"
          case None => down.cause
        }
    )
}
