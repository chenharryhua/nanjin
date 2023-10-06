package com.github.chenharryhua.nanjin.guard.observers
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, NJEvent}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive

import java.time.{Duration, Instant}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

object sampling {

  /** interval based sampling
    *
    * in every interval, only one MetricReport is allowed to pass
    */
  def apply(interval: FiniteDuration)(evt: NJEvent): Boolean =
    evt match {
      case MetricReport(mrt, sp, now, _) =>
        mrt match {
          case MetricIndex.Adhoc => true
          case MetricIndex.Periodic(tick) =>
            val expect: Instant =
              sp.launchTime
                .plus(((Duration.between(sp.launchTime, now).toScala / interval).toLong * interval).toJava)
                .toInstant
            tick.inBetween(expect)
        }
      case _ => true
    }

  /** index based sampling
    *
    * report index mod divisor === 0
    */
  def apply(divisor: Refined[Int, Positive])(evt: NJEvent): Boolean =
    evt match {
      case MetricReport(mrt, _, _, _) =>
        mrt match {
          case MetricIndex.Adhoc          => true
          case MetricIndex.Periodic(tick) => (tick.index % divisor.value) === 0
        }
      case _ => true
    }

  /** cron based sampling
    */
  def apply(cronExpr: CronExpr)(evt: NJEvent): Boolean =
    evt match {
      case MetricReport(mrt, sp, _, _) =>
        mrt match {
          case MetricIndex.Adhoc => true
          case MetricIndex.Periodic(tick) =>
            cronExpr.next(sp.toZonedDateTime(tick.previous)).exists(zdt => tick.inBetween(zdt.toInstant))
        }
      case _ => true
    }

}
