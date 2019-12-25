package com.github.chenharryhua.nanjin

import java.time.LocalDateTime
import java.util.Properties

import cats.Eval

import scala.util.Random
package object utils {
  //kafka was graduated from apache incubator
  val kafkaEpoch: LocalDateTime = LocalDateTime.of(2012, 10, 23, 0, 0, 0)

  def toProperties(props: Map[String, String]): Properties =
    props.foldLeft(new Properties()) { case (a, (k, v)) => a.put(k, v); a }

  val random4d: Eval[Int]          = Eval.always(1000 + Random.nextInt(9000))
  val defaultLocalParallelism: Int = Runtime.getRuntime.availableProcessors()
}
