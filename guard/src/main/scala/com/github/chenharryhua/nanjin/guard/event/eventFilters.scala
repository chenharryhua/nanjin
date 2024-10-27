package com.github.chenharryhua.nanjin.guard.event

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.crontabs
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import cron4s.syntax.all.*
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive

import java.time.{Duration, Instant}
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

object eventFilters {

  /** interval based sampling
    *
    * in every interval, only one MetricReport is allowed to pass
    */
  def sampling(interval: FiniteDuration)(evt: NJEvent): Boolean =
    evt match {
      case MetricReport(mrt, sp, _, _) =>
        mrt match {
          case MetricIndex.Adhoc(_) => true
          case MetricIndex.Periodic(tick) =>
            val expect: Instant =
              sp.zerothTick.launchTime.plus(
                ((Duration
                  .between(sp.zerothTick.launchTime, tick.wakeup)
                  .toScala / interval).toLong * interval).toJava)

            tick.inBetween(expect)
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
          case MetricIndex.Adhoc(_)       => true
          case MetricIndex.Periodic(tick) => (tick.index % divisor.value) === 0
        }
      case _ => true
    }

  /** cron based sampling
    */
  def sampling(cronExpr: CronExpr)(evt: NJEvent): Boolean =
    evt match {
      case MetricReport(mrt, _, _, _) =>
        mrt match {
          case MetricIndex.Adhoc(_) => true
          case MetricIndex.Periodic(tick) =>
            cronExpr.next(tick.zonedPrevious).exists(zdt => tick.inBetween(zdt.toInstant))
        }
      case _ => true
    }

  def sampling(f: crontabs.type => CronExpr)(evt: NJEvent): Boolean =
    sampling(f(crontabs))(evt)

}
