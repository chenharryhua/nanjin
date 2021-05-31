package com.github.chenharryhua.nanjin.common

import cats.Eval
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.commons.lang3.time.DurationFormatUtils

import java.time.{Instant, LocalDateTime, Duration => JavaDuration}
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Random

object utils {
  val zzffEpoch: LocalDateTime = LocalDateTime.of(2012, 10, 26, 18, 0, 0)

  //kafka was graduated from apache incubator
  val kafkaEpoch: LocalDateTime = LocalDateTime.of(2012, 10, 23, 0, 0, 0)
  val sparkEpoch: LocalDateTime = LocalDateTime.of(2014, 2, 1, 0, 0, 0)
  val flinkEpoch: LocalDateTime = LocalDateTime.of(2014, 12, 1, 0, 0, 0)

  def toProperties(props: Map[String, String]): Properties =
    props.foldLeft(new Properties()) { case (a, (k, v)) => a.put(k, v); a }

  val random4d: Eval[Int]          = Eval.always(1000 + Random.nextInt(9000))
  val defaultLocalParallelism: Int = Runtime.getRuntime.availableProcessors()

  def mkExceptionString(err: Throwable, lines: Int = 2): String =
    ExceptionUtils.getRootCauseStackTrace(err).take(lines).mkString("\n")

  final val oneSecond: FiniteDuration   = Duration(1, TimeUnit.SECONDS)
  final val oneMilliSec: FiniteDuration = Duration(1, TimeUnit.MILLISECONDS)

  def mkDurationString(dur: Duration): String =
    if (dur < oneMilliSec) s"${dur.toNanos} nanoseconds"
    else if (dur < oneSecond) s"${dur.toMillis} milliseconds"
    else DurationFormatUtils.formatDurationWords(dur.toMillis, true, true)

  def mkDurationString(start: Instant, end: Instant): String =
    mkDurationString(FiniteDuration(Math.abs(JavaDuration.between(start, end).toNanos), TimeUnit.NANOSECONDS))

}
