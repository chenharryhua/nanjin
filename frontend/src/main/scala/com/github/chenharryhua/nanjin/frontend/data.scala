package com.github.chenharryhua.nanjin.frontend

import io.circe.Codec

import scala.scalajs.js
import scala.scalajs.js.JSConverters.*

case class Series(label: String, value: Double) derives Codec.AsObject

case class WsMessage(ts: Double, series: List[Series]) derives Codec.AsObject {
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
