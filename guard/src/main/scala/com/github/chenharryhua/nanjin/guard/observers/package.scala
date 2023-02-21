package com.github.chenharryhua.nanjin.guard

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, NJEvent}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import cron4s.lib.javatime.javaTemporalInstance
import cron4s.CronExpr
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import org.typelevel.cats.time.instances.zoneddatetime.*

import java.time.{Duration, ZonedDateTime}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

package object observers {
  final val METRICS_CATEGORY: String = "category"

  final val METRICS_RATE_UNIT: String     = "rate_unit"
  final val METRICS_DURATION_UNIT: String = "duration_unit"

  final val METRICS_COUNT: String = "count"

  final val METRICS_MEAN_RATE: String      = "mean_rate"
  final val METRICS_1_MINUTE_RATE: String  = "1_minute_rate"
  final val METRICS_5_MINUTE_RATE: String  = "5_minute_rate"
  final val METRICS_15_MINUTE_RATE: String = "15_minute_rate"

  final val METRICS_MIN: String     = "min"
  final val METRICS_MAX: String     = "max"
  final val METRICS_MEAN: String    = "mean"
  final val METRICS_STD_DEV: String = "stddev"
  final val METRICS_MEDIAN: String  = "median"

  final val METRICS_P75: String  = "p75"
  final val METRICS_P95: String  = "p95"
  final val METRICS_P98: String  = "p98"
  final val METRICS_P99: String  = "p99"
  final val METRICS_P999: String = "p999"

  final val METRICS_TASK: String        = "Task"
  final val METRICS_SERVICE: String     = "Service"
  final val METRICS_SERVICE_ID: String  = "ServiceID"
  final val METRICS_HOST: String        = "Host"
  final val METRICS_LAUNCH_TIME: String = "LaunchTime"

  /** interval based sampling
    *
    * in every interval, only one MetricReport is allowed to pass
    */
  def sampling(interval: FiniteDuration)(evt: NJEvent): Boolean =
    evt match {
      case MetricReport(mrt, sp, now, _) =>
        mrt match {
          case MetricIndex.Adhoc => true
          case MetricIndex.Periodic(_) =>
            val border: ZonedDateTime =
              sp.launchTime.plus(
                ((Duration.between(sp.launchTime, now).toScala / interval).toLong * interval).toJava)
            if (now === border) true
            else
              sp.metricParams.reportSchedule match {
                case None     => true
                case Some(ce) => ce.prev(now).forall(_.isBefore(border)) && now.isAfter(border)
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
          case MetricIndex.Adhoc           => true
          case MetricIndex.Periodic(index) => (index % divisor.value) === 0
        }
      case _ => true
    }

  /** cron based sampling
    */
  def sampling(cronExpr: CronExpr)(evt: NJEvent): Boolean =
    evt match {
      case MetricReport(mrt, sp, now, _) =>
        mrt match {
          case MetricIndex.Adhoc => true
          case MetricIndex.Periodic(_) =>
            val nextReport = sp.metricParams.nextReport(now)
            val nextBorder = cronExpr.next(now)
            (nextReport, nextBorder).mapN((r, b) => !r.isBefore(b)).exists(identity)
        }
      case _ => true
    }
}
