package com.github.chenharryhua.nanjin.guard.event

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.crontabs
import com.github.chenharryhua.nanjin.guard.event.Event.MetricReport
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import cron4s.syntax.all.*
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import monocle.macros.GenPrism

import java.time.{Duration, Instant}
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

object eventFilters {

  /** interval based sampling
    *
    * in every interval, only one MetricReport is allowed to pass
    */
  def sampling(interval: FiniteDuration)(evt: Event): Boolean =
    evt match {
      case MetricReport(mrt, sp, _, _) =>
        mrt match {
          case MetricIndex.Adhoc(_)       => true
          case MetricIndex.Periodic(tick) =>
            val expect: Instant =
              sp.zerothTick.launchTime.plus(
                ((Duration
                  .between(sp.zerothTick.launchTime, tick.conclude)
                  .toScala / interval).toLong * interval).toJava)
            tick.isWithinOpenClosed(expect)
        }
      case _ => true
    }

  /** index based sampling
    *
    * report index mod divisor === 0
    */
  def sampling(divisor: Refined[Int, Positive])(evt: Event): Boolean =
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
  def sampling(cronExpr: CronExpr)(evt: Event): Boolean =
    evt match {
      case MetricReport(mrt, _, _, _) =>
        mrt match {
          case MetricIndex.Adhoc(_)       => true
          case MetricIndex.Periodic(tick) =>
            cronExpr.next(tick.zoned(_.commence)).exists(zdt => tick.isWithinOpenClosed(zdt.toInstant))
        }
      case _ => true
    }

  def sampling(f: crontabs.type => CronExpr)(evt: Event): Boolean =
    sampling(f(crontabs))(evt)

  // mapFilter friendly

  val metricReport: Event => Option[Event.MetricReport] =
    GenPrism[Event, Event.MetricReport].getOption(_)

  val metricReset: Event => Option[Event.MetricReset] =
    GenPrism[Event, Event.MetricReset].getOption(_)

  val serviceMessage: Event => Option[Event.ServiceMessage] =
    GenPrism[Event, Event.ServiceMessage].getOption(_)

  val serviceStart: Event => Option[Event.ServiceStart] =
    GenPrism[Event, Event.ServiceStart].getOption(_)

  val serviceStop: Event => Option[Event.ServiceStop] =
    GenPrism[Event, Event.ServiceStop].getOption(_)

  val servicePanic: Event => Option[Event.ServicePanic] =
    GenPrism[Event, Event.ServicePanic].getOption(_)

}
