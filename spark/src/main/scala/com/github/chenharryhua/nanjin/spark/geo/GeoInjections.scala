package com.github.chenharryhua.nanjin.spark.geo

import frameless.Injection
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, Polygon}

final case class NJCoordinate(getX: Double, getY: Double, getZ: Double)

final case class NJPoint(getX: Double, getY: Double)

final case class NJPolygon(coordinates: Array[Coordinate])

private[geo] trait GeoInjections {

  implicit val coordinateInjection: Injection[Coordinate, NJCoordinate] =
    new Injection[Coordinate, NJCoordinate] {
      override def apply(a: Coordinate): NJCoordinate = NJCoordinate(a.getX, a.getY, a.getZ)

      override def invert(b: NJCoordinate): Coordinate =
        new Coordinate(b.getX, b.getY, b.getZ)
    }

  implicit val pointInjection: Injection[Point, NJPoint] =
    new Injection[Point, NJPoint] {
      override def apply(a: Point): NJPoint = NJPoint(a.getX, a.getY)

      override def invert(b: NJPoint): Point = {
        val factory = new GeometryFactory()
        factory.createPoint(new Coordinate(b.getX, b.getY))
      }
    }

  implicit val polygonInjection: Injection[Polygon, NJPolygon] =
    new Injection[Polygon, NJPolygon] {
      override def apply(a: Polygon): NJPolygon = NJPolygon(a.getCoordinates)

      override def invert(b: NJPolygon): Polygon = {
        val factory = new GeometryFactory()
        factory.createPolygon(b.coordinates)
      }
    }
}
