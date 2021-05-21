package com.github.chenharryhua.nanjin.common

import cats.Eval
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.commons.lang3.time.DurationFormatUtils

import java.time.{Instant, LocalDateTime}
import java.util.Properties
import scala.concurrent.duration.Duration
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

  def mkDurationString(millis: Long): String =
    DurationFormatUtils.formatDurationWords(millis, true, true)

  def mkDurationString(dur: Duration): String =
    mkDurationString(dur.toMillis)

  def mkDurationString(start: Instant, end: Instant): String =
    mkDurationString(end.toEpochMilli - start.toEpochMilli)

}
