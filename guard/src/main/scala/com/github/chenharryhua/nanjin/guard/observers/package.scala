package com.github.chenharryhua.nanjin.guard

import cats.syntax.all.*
import com.codahale.metrics.MetricAttribute
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, NJEvent}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive

import java.time.{Duration, Instant}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

package object observers {

  // counters
  @inline final val METRICS_COUNT: String = MetricAttribute.COUNT.getCode

  // meters
  @inline final val METRICS_MEAN_RATE: String      = MetricAttribute.MEAN_RATE.getCode
  @inline final val METRICS_1_MINUTE_RATE: String  = MetricAttribute.M1_RATE.getCode
  @inline final val METRICS_5_MINUTE_RATE: String  = MetricAttribute.M5_RATE.getCode
  @inline final val METRICS_15_MINUTE_RATE: String = MetricAttribute.M15_RATE.getCode

  // histograms
  @inline final val METRICS_MIN: String     = MetricAttribute.MIN.getCode
  @inline final val METRICS_MAX: String     = MetricAttribute.MAX.getCode
  @inline final val METRICS_MEAN: String    = MetricAttribute.MEAN.getCode
  @inline final val METRICS_STD_DEV: String = MetricAttribute.STDDEV.getCode

  @inline final val METRICS_P50: String  = MetricAttribute.P50.getCode
  @inline final val METRICS_P75: String  = MetricAttribute.P75.getCode
  @inline final val METRICS_P95: String  = MetricAttribute.P95.getCode
  @inline final val METRICS_P98: String  = MetricAttribute.P98.getCode
  @inline final val METRICS_P99: String  = MetricAttribute.P99.getCode
  @inline final val METRICS_P999: String = MetricAttribute.P999.getCode

  // dimensions
  @inline final val METRICS_LAUNCH_TIME: String = "LaunchTime"
  @inline final val METRICS_CATEGORY: String    = "Category"
  @inline final val METRICS_DIGEST: String      = "Digest"
  @inline final val METRICS_NAME: String        = "MetricName"

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
