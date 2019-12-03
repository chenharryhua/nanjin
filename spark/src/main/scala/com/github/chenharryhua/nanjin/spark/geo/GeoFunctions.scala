package com.github.chenharryhua.nanjin.spark.geo

import frameless.{Injection, TypedColumn, TypedDataset}
import org.locationtech.jts.geom.{Geometry, Point, Polygon}
import frameless.functions.udf

private[geo] trait GeoFunctions extends GeoInjections with Serializable {

  /** Example:
    * {{{
    *  val ds1: TypedDataset[Polygon] = ???
    *  val ds2: TypedDataset[Point] = ???
    *  val joined: TypedDataset[(Polygon, Point)] = ds1.joinCross(ds2)
    *
    *  val rst = joined.filter(covers(joined('_1), joined('_2)))
    * }}}
    */

  def covers[T]: (TypedColumn[T, Polygon], TypedColumn[T, Point]) => TypedColumn[T, Boolean] =
    udf[T, Polygon, Point, Boolean]((polygon: Polygon, point: Point) => polygon.covers(point))
}
