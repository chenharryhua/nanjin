package com.github.chenharryhua.nanjin

import cats.Eval
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.LocalDateTime
import java.util.Properties
import scala.util.Random

package object utils {
  val zzffEpoch: LocalDateTime = LocalDateTime.of(2012, 10, 26, 18, 0, 0)

  //kafka was graduated from apache incubator
  val kafkaEpoch: LocalDateTime = LocalDateTime.of(2012, 10, 23, 0, 0, 0)
  val sparkEpoch: LocalDateTime = LocalDateTime.of(2014, 2, 1, 0, 0, 0)
  val flinkEpoch: LocalDateTime = LocalDateTime.of(2014, 12, 1, 0, 0, 0)

  def toProperties(props: Map[String, String]): Properties =
    props.foldLeft(new Properties()) { case (a, (k, v)) => a.put(k, v); a }

  val random4d: Eval[Int]          = Eval.always(1000 + Random.nextInt(9000))
  val defaultLocalParallelism: Int = Runtime.getRuntime.availableProcessors()

  def mkString(err: Throwable, lines: Int = 8): String =
    ExceptionUtils.getRootCauseStackTrace(err).take(lines).mkString("\n")

}
