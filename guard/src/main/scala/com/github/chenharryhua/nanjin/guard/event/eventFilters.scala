package com.github.chenharryhua.nanjin.guard.event

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.crontabs
import com.github.chenharryhua.nanjin.guard.config.Importance
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionDone, ActionEvent, ActionStart, MetricReport}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import cron4s.syntax.all.*
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive

import java.time.{Duration, Instant}
import scala.concurrent.duration.*
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
      case MetricReport(mrt, _, _, _) =>
        mrt match {
          case MetricIndex.Adhoc => true
          case MetricIndex.Periodic(tick) =>
            cronExpr.next(tick.previous.atZone(tick.zoneId)).exists(zdt => tick.inBetween(zdt.toInstant))
        }
      case _ => true
    }

  def sampling(f: crontabs.type => CronExpr)(evt: NJEvent): Boolean =
    sampling(f(crontabs))(evt)

}
