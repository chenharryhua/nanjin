package com.github.chenharryhua.nanjin.guard.event

import cats.syntax.all.*
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

  /** Interval-based sampling.
    *
    * In each `interval`, only one `MetricReport` is allowed to pass. Adhoc reports (`MetricIndex.Adhoc`)
    * always pass. Periodic reports are checked if their tick falls within the expected open-closed interval.
    *
    * @param interval
    *   the sampling interval
    * @param evt
    *   the event to test
    * @return
    *   `true` if the event should pass, `false` otherwise
    */
  def sampling(interval: FiniteDuration)(evt: Event): Boolean =
    evt match {
      case MetricReport(mrt, sp, _, _) =>
        mrt match {
          case MetricIndex.Adhoc(_)       => true
          case MetricIndex.Periodic(tick) =>
            val expect: Instant =
              sp.launchTime.toInstant.plus(
                ((Duration
                  .between(sp.launchTime.toInstant, tick.conclude)
                  .toScala / interval).toLong * interval).toJava)
            tick.isWithinOpenClosed(expect)
        }
      case _ => true
    }

  /** Index-based sampling.
    *
    * Only `MetricReport` events whose index modulo `divisor` is zero will pass. Adhoc reports always pass.
    *
    * @param divisor
    *   a positive integer refined type used as the modulo divisor
    * @param evt
    *   the event to test
    * @return
    *   `true` if the event should pass, `false` otherwise
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

  /** Cron-based sampling.
    *
    * Only `MetricReport` events that match the given cron expression will pass. Adhoc reports always pass.
    *
    * @param f
    *   the cron expression defining the schedule
    * @param evt
    *   the event to test
    * @return
    *   `true` if the event should pass, `false` otherwise
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

  // --------------------------------------------------------------------------
  // MapFilter-friendly accessors
  // --------------------------------------------------------------------------

  val metricReport: Event => Option[Event.MetricReport] =
    GenPrism[Event, Event.MetricReport].getOption

  val metricReset: Event => Option[Event.MetricReset] =
    GenPrism[Event, Event.MetricReset].getOption

  val serviceMessage: Event => Option[Event.ServiceMessage] =
    GenPrism[Event, Event.ServiceMessage].getOption

  val serviceStart: Event => Option[Event.ServiceStart] =
    GenPrism[Event, Event.ServiceStart].getOption

  val serviceStop: Event => Option[Event.ServiceStop] =
    GenPrism[Event, Event.ServiceStop].getOption

  val servicePanic: Event => Option[Event.ServicePanic] =
    GenPrism[Event, Event.ServicePanic].getOption

}
