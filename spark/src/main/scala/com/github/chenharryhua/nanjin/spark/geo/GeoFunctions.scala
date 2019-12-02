package com.github.chenharryhua.nanjin.spark.geo

import frameless.{Injection, TypedColumn, TypedDataset}
import org.locationtech.jts.geom.{Geometry, Point, Polygon}
import frameless.functions.udf

private[geo] trait GeoFunctions extends GeoInjections with Serializable {

  def contains: (
    TypedColumn[Geometry, Polygon],
    TypedColumn[Geometry, Point]) => TypedColumn[Geometry, Boolean] =
    udf[Geometry, Polygon, Point, Boolean]((polygon: Polygon, point: Point) =>
      polygon.covers(point))
  def contains2: TypedColumn[Polygon with Point, Boolean] = ???

}
