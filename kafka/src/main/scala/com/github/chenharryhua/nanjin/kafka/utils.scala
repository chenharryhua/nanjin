package com.github.chenharryhua.nanjin.kafka

import java.util.Properties

object utils {

  def toProperties(props: Map[String, String]): Properties =
    (new Properties() /: props) { case (a, (k, v)) => a.put(k, v); a }

}
