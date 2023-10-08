package com.github.chenharryhua.nanjin.guard.event

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.Importance
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{
  ActionDone,
  ActionEvent,
  ActionFail,
  ActionResultEvent,
  ActionStart,
  MetricReport
}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive

import java.time.{Duration, Instant}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

object eventFilters {
  final def isPivotalEvent(evt: NJEvent): Boolean = evt match {
    case _: ActionDone  => false
    case _: ActionStart => false
    case _              => true
  }

  final def isServiceEvent(evt: NJEvent): Boolean = evt match {
    case _: ActionEvent => false
    case _              => true
  }

  final def isActionDone(evt: ActionResultEvent): Boolean = evt match {
    case _: ActionFail => false
    case _: ActionDone => true
  }

  final def nonSuppress(evt: NJEvent): Boolean = evt match {
    case ae: ActionEvent => ae.actionParams.importance > Importance.Suppressed
    case _               => true
  }

  /** interval based sampling
    *
    * in every interval, only one MetricReport is allowed to pass
    */
  def sampling(interval: FiniteDuration)(evt: NJEvent): Boolean =
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
  def sampling(divisor: Refined[Int, Positive])(evt: NJEvent): Boolean =
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
  def sampling(cronExpr: CronExpr)(evt: NJEvent): Boolean =
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
