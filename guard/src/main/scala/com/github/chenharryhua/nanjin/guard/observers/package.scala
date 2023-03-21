package com.github.chenharryhua.nanjin.guard

import cats.syntax.all.*
import com.codahale.metrics.MetricAttribute
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, NJEvent}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import org.typelevel.cats.time.instances.zoneddatetime.*

import java.time.{Duration, ZonedDateTime}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

package object observers {

  // counters
  final val METRICS_COUNT: String = MetricAttribute.COUNT.getCode

  // meters
  final val METRICS_MEAN_RATE: String      = MetricAttribute.MEAN_RATE.getCode
  final val METRICS_1_MINUTE_RATE: String  = MetricAttribute.M1_RATE.getCode
  final val METRICS_5_MINUTE_RATE: String  = MetricAttribute.M5_RATE.getCode
  final val METRICS_15_MINUTE_RATE: String = MetricAttribute.M15_RATE.getCode

  // histograms
  final val METRICS_MIN: String     = MetricAttribute.MIN.getCode
  final val METRICS_MAX: String     = MetricAttribute.MAX.getCode
  final val METRICS_MEAN: String    = MetricAttribute.MEAN.getCode
  final val METRICS_STD_DEV: String = MetricAttribute.STDDEV.getCode

  final val METRICS_P50: String  = MetricAttribute.P50.getCode
  final val METRICS_P75: String  = MetricAttribute.P75.getCode
  final val METRICS_P95: String  = MetricAttribute.P95.getCode
  final val METRICS_P98: String  = MetricAttribute.P98.getCode
  final val METRICS_P99: String  = MetricAttribute.P99.getCode
  final val METRICS_P999: String = MetricAttribute.P999.getCode

  // dimensions
  final val METRICS_TASK: String        = "Task"
  final val METRICS_SERVICE: String     = "Service"
  final val METRICS_SERVICE_ID: String  = "ServiceID"
  final val METRICS_HOST: String        = "Host"
  final val METRICS_LAUNCH_TIME: String = "LaunchTime"
  final val METRICS_CATEGORY: String    = "Category"
  final val METRICS_DIGEST: String      = "Digest"
  final val METRICS_MEASUREMENT: String = "Measurement"

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
