package com.github.chenharryhua.nanjin.spark.geo

import frameless.TypedColumn
import frameless.functions.udf
import monocle.Lens
import org.locationtech.jts.geom.{Point, Polygon}

final class PolygonCollection[A](polygons: List[A], lens: Lens[A, Polygon]) {
  private val listPairs: List[(A, Polygon)] = polygons.map(p => (p, lens.get(p)))

  @scala.annotation.tailrec
  private def findContainingPolygon(point: Point, polygons: List[(A, Polygon)]): Option[A] =
    polygons match {
      case (a, polygon) :: rest =>
        if (polygon.covers(point)) Some(a) else findContainingPolygon(point, rest)
      case Nil => None
    }

  def find(point: Point): Option[A] = findContainingPolygon(point, listPairs)
}

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

  def intersects[T]: (TypedColumn[T, Polygon], TypedColumn[T, Polygon]) => TypedColumn[T, Boolean] =
    udf[T, Polygon, Polygon, Boolean]((polygon: Polygon, other: Polygon) =>
      polygon.intersects(other))

}
