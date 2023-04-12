package com.github.chenharryhua.nanjin.guard

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, NJEvent}
import org.apache.commons.lang3.StringUtils
import org.typelevel.cats.time.instances.localdatetime.localdatetimeInstances
import org.typelevel.cats.time.instances.localtime.localtimeInstances

import java.text.NumberFormat
import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}

package object translators {
  @inline final val CONSTANT_ACTION_ID: String  = "ActionID"
  @inline final val CONSTANT_TRACE_ID: String   = "TraceID"
  @inline final val CONSTANT_TIMESTAMP: String  = "Timestamp"
  @inline final val CONSTANT_POLICY: String     = "Policy"
  @inline final val CONSTANT_CAUSE: String      = "Cause"
  @inline final val CONSTANT_TOOK: String       = "Took"
  @inline final val CONSTANT_DELAYED: String    = "Delayed"
  @inline final val CONSTANT_NOTES: String      = "Notes"
  @inline final val CONSTANT_RESULT: String     = "Result"
  @inline final val CONSTANT_IMPORTANCE: String = "Importance"
  @inline final val CONSTANT_UPTIME: String     = "UpTime"
  @inline final val CONSTANT_BRIEF: String      = "Brief"
  @inline final val CONSTANT_METRICS: String    = "Metrics"
  @inline final val CONSTANT_TIMEZONE: String   = "TimeZone"
  @inline final val CONSTANT_SERVICE: String    = "Service"
  @inline final val CONSTANT_SERVICE_ID: String = "ServiceID"
  @inline final val CONSTANT_HOST: String       = "Host"
  @inline final val CONSTANT_TASK: String       = "Task"

  // slack not allow message larger than 3000 chars
  // https://api.slack.com/reference/surfaces/formatting
  final private[translators] val MessageSizeLimits: Int = 2500

  final private[translators] def abbreviate(msg: String): String =
    StringUtils.abbreviate(msg, MessageSizeLimits)

  final def toOrdinalWords(n: Int): String = {
    val w =
      if (n % 100 / 10 == 1) "th"
      else {
        n % 10 match {
          case 1 => "st"
          case 2 => "nd"
          case 3 => "rd"
          case _ => "th"
        }
      }
    s"$n$w"
  }

  // NumberFormat and DurationFormatter are thread safe
  final private[translators] val fmt: DurationFormatter = DurationFormatter.defaultFormatter
  final private[translators] val numFmt: NumberFormat   = NumberFormat.getInstance()

  final private[translators] def localTimeAndDurationStr(
    start: ZonedDateTime,
    end: ZonedDateTime): (String, String) = {
    val duration = Duration.between(start, end)
    val localTime: String =
      if (duration.minus(Duration.ofHours(24)).isNegative)
        end.truncatedTo(ChronoUnit.SECONDS).toLocalTime.show
      else
        end.truncatedTo(ChronoUnit.SECONDS).toLocalDateTime.show

    (localTime, fmt.format(duration))
  }

  final private[translators] def eventTitle(evt: NJEvent): String =
    evt match {
      case NJEvent.ActionStart(ap, _)             => s"Start Action ${ap.metricId.metricName.display}"
      case NJEvent.ActionRetry(ap, _, _, _, _, _) => s"Action Retrying ${ap.metricId.metricName.display}"
      case NJEvent.ActionFail(ap, _, _, _, _)     => s"Action Failed ${ap.metricId.metricName.display}"
      case NJEvent.ActionComplete(ap, _, _, _)    => s"Action Completed ${ap.metricId.metricName.display}"

      case NJEvent.InstantAlert(metricName, _, _, _, _) => s"Alert ${metricName.display}"

      case _: NJEvent.ServiceStart => "(Re)Start Service"
      case _: NJEvent.ServiceStop  => "Service Stopped"
      case _: NJEvent.ServicePanic => "Service Panic"

      case NJEvent.MetricReport(index, _, _, _) =>
        index match {
          case MetricIndex.Adhoc         => "Adhoc Metric Report"
          case MetricIndex.Periodic(idx) => s"Metric Report(index=$idx)"
        }
      case NJEvent.MetricReset(index, _, _, _) =>
        index match {
          case MetricIndex.Adhoc         => "Adhoc Metric Reset"
          case MetricIndex.Periodic(idx) => s"Metric Reset(index=$idx)"
        }
    }

}
