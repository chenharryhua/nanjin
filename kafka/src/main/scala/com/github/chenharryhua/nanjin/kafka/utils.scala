package com.github.chenharryhua.nanjin.kafka

import java.util.Properties

import cats.Eval

import scala.util.Random

object utils {

  def toProperties(props: Map[String, String]): Properties =
    (new Properties() /: props) { case (a, (k, v)) => a.put(k, v); a }

  val random4d: Eval[Int] = Eval.always(1000 + Random.nextInt(9000))

}
