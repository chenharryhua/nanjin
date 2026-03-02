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
case class Series(name: String, point: Point)

@JsonCodec
case class WsMessage(series: List[Series])
