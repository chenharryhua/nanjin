package com.github.chenharryhua.nanjin.common

import cats.Eval

import java.time.LocalDateTime
import java.util.Properties
import scala.util.Random

object utils {

  final val epoch: LocalDateTime = LocalDateTime.of(2019, 7, 21, 0, 0, 0)

  // kafka was graduated from apache incubator
  final val kafkaEpoch: LocalDateTime = LocalDateTime.of(2012, 10, 23, 0, 0, 0)
  final val sparkEpoch: LocalDateTime = LocalDateTime.of(2014, 2, 1, 0, 0, 0)
  final val flinkEpoch: LocalDateTime = LocalDateTime.of(2014, 12, 1, 0, 0, 0)

  def toProperties(props: Map[String, String]): Properties = {
    val p = new Properties()
    props.foreach { case (k, v) => p.setProperty(k, v) }
    p
  }

  final val random4d: Eval[Int] = Eval.always(1000 + Random.nextInt(9000))
}
