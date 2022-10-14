package com.github.chenharryhua.nanjin.guard

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.{ScheduleType, ServiceParams}
import cron4s.lib.javatime.javaTemporalInstance
import org.apache.commons.lang3.StringUtils
import org.typelevel.cats.time.instances.localdatetime.localdatetimeInstances
import org.typelevel.cats.time.instances.localtime.localtimeInstances

import java.time.{Duration, ZonedDateTime}
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

package object translators {
  import ScheduleType.*

  def nextTime(
    reportSchedule: Option[ScheduleType],
    now: ZonedDateTime,
    interval: Option[FiniteDuration],
    launchTime: ZonedDateTime): Option[ZonedDateTime] = {
    val nextBorder: Option[ZonedDateTime] =
      interval.map(iv =>
        launchTime.plus((((Duration.between(launchTime, now).toScala / iv).toLong + 1) * iv).toJava))
    nextBorder match {
      case None =>
        reportSchedule.flatMap {
          case Fixed(fd) => Some(now.plus(fd))
          case Cron(ce)  => ce.next(now)
        }
      case Some(b) =>
        reportSchedule.flatMap {
          case Fixed(fd) =>
            LazyList.iterate(now)(_.plus(fd)).dropWhile(_.isBefore(b)).headOption
          case Cron(ce) =>
            LazyList.unfold(now)(dt => ce.next(dt).map(t => Tuple2(t, t))).dropWhile(_.isBefore(b)).headOption
        }
    }
  }

  // slack not allow message larger than 3000 chars
  // https://api.slack.com/reference/surfaces/formatting
  final val MessageSizeLimits: Int = 2500

  private[translators] def abbreviate(msg: String): String = StringUtils.abbreviate(msg, MessageSizeLimits)

  private[translators] def hostServiceSection(sp: ServiceParams): JuxtaposeSection = {
    val sn: String =
      sp.taskParams.homePage.fold(sp.serviceName.value)(hp => s"<${hp.value}|${sp.serviceName.value}>")
    JuxtaposeSection(TextField("Service", sn), TextField("Host", sp.taskParams.hostName.value))
  }

  def toOrdinalWords(n: Int): String = {
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

  private[translators] def localTimeAndDurationStr(
    start: ZonedDateTime,
    end: ZonedDateTime): (String, String) = {
    val duration = Duration.between(start, end)
    val localTime: String =
      if (duration.minus(Duration.ofHours(24)).isNegative)
        end.truncatedTo(ChronoUnit.SECONDS).toLocalTime.show
      else
        end.truncatedTo(ChronoUnit.SECONDS).toLocalDateTime.show

    (localTime, fmt.format(duration))
  }

}
