package com.github.chenharryhua.nanjin.guard

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.ScheduleType
import com.github.chenharryhua.nanjin.guard.event.{MetricReportType, NJEvent}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import cron4s.lib.javatime.javaTemporalInstance
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import org.typelevel.cats.time.instances.zoneddatetime.*

import java.time.{Duration, ZonedDateTime}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

package object observers {

  /** interval based sampling
    *
    * in every interval, only one MetricReport is allowed to pass
    */
  def sampling(interval: FiniteDuration)(evt: NJEvent): Boolean =
    evt match {
      case MetricReport(mrt, sp, now, _) =>
        mrt match {
          case MetricReportType.Adhoc => true
          case MetricReportType.Scheduled(_) =>
            val border: ZonedDateTime =
              sp.launchTime.plus(
                ((Duration.between(sp.launchTime, now).toScala / interval).toLong * interval).toJava)
            if (now === border) true
            else
              sp.metricParams.reportSchedule match {
                case None => true
                // true when now cross the border
                case Some(ScheduleType.Fixed(fd)) => now.minus(fd).isBefore(border) && now.isAfter(border)
                case Some(ScheduleType.Cron(ce)) =>
                  ce.prev(now).forall(_.isBefore(border)) && now.isAfter(border)
              }
        }
      case _ => true
    }

  /** index based sampling
    *
    * report index mod divisor === 0
    */
  def sampling(divisor: Refined[Int, Positive])(evt: NJEvent): Boolean =
    evt match {
      case MetricReport(mrt, _, _, _) =>
        mrt match {
          case MetricReportType.Adhoc            => true
          case MetricReportType.Scheduled(index) => (index % divisor.value) === 0
        }
      case _ => true
    }
}
