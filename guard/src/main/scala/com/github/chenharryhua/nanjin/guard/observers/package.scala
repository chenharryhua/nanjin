package com.github.chenharryhua.nanjin.guard

import cats.syntax.all.*
import com.amazonaws.thirdparty.apache.codec.digest.DigestUtils
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import org.apache.commons.lang3.StringUtils

import java.time.{Duration, ZonedDateTime}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

package object observers {
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

  def isShowMetrics(
    reportSchedule: Option[Either[FiniteDuration, CronExpr]],
    now: ZonedDateTime,
    interval: Option[FiniteDuration],
    launchTime: ZonedDateTime): Boolean =
    interval match {
      case None => true
      case Some(iv) =>
        val border: ZonedDateTime =
          launchTime.plus(((Duration.between(launchTime, now).toScala / iv).toLong * iv).toJava)
        if (now === border) true
        else
          reportSchedule match {
            case None => true
            // true when now cross the border
            case Some(Left(fd))  => now.minus(fd.toJava).isBefore(border) && now.isAfter(border)
            case Some(Right(ce)) => ce.prev(now).forall(_.isBefore(border) && now.isAfter(border))
          }
    }

  // slack not allow message larger than 3000 chars
  // https://api.slack.com/reference/surfaces/formatting
  final val MessageSizeLimits: Int = 2960

  private[observers] def abbreviate(msg: String): String = StringUtils.abbreviate(msg, MessageSizeLimits)

  private[observers] def hostServiceSection(sp: ServiceParams): JuxtaposeSection =
    JuxtaposeSection(TextField("Service", sp.metricName.value), TextField("Host", sp.taskParams.hostName))

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
}
