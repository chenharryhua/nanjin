package com.github.chenharryhua.nanjin.frontend

import io.circe.generic.JsonCodec

import scala.scalajs.js
import scala.scalajs.js.JSConverters._

@JsonCodec
case class Series(label: String, value: Double)

@JsonCodec
case class WsMessage(series: List[Series], ts: Double) {
  val points: Map[String, Point] = series.map { case Series(label, value) =>
    label -> Point(ts, Some(value))
  }.toMap
}

final case class Point(x: Double, y: Option[Double]) {
  def dataPoint: js.Dynamic = js.Dynamic.literal(
    x = x,
    y = y.orUndefined
  )
}
