package com.github.chenharryhua.nanjin.frontend

import io.circe.generic.JsonCodec

import scala.scalajs.js

@JsonCodec
case class Point(x: Double, y: Double) {
  def dataPoint: js.Dynamic = js.Dynamic.literal(
    x = x,
    y = y
  )
}

@JsonCodec
case class Series(name: String, point: Point) {
  def toDataset(borderColor: String): js.Dynamic = js.Dynamic.literal(
    label = name,
    data = js.Array(point.dataPoint),
    borderColor = borderColor,
    fill = false,
    tension = 0.3,
    borderWidth = 2,
    pointRadius = 0
  )
}
@JsonCodec
case class WsMessage(series: List[Series])
