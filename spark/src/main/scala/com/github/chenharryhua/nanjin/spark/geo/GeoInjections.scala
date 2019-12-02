package com.github.chenharryhua.nanjin.spark.geo

import frameless.Injection
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, Polygon}

final case class NJCoordinate(latitude: Double, longtitude: Double)
final case class NJPoint(latitude: Double, longtitude: Double)
final case class NJPolygon(value: Array[Coordinate])

private[geo] trait GeoInjections {

  implicit val coordinateInjection: Injection[Coordinate, NJCoordinate] =
    new Injection[Coordinate, NJCoordinate] {
      override def apply(a: Coordinate): NJCoordinate = NJCoordinate(a.getY, a.getX)

      override def invert(b: NJCoordinate): Coordinate =
        new Coordinate(b.longtitude, b.latitude)
    }

  implicit val pointInjection: Injection[Point, NJPoint] =
    new Injection[Point, NJPoint] {
      override def apply(a: Point): NJPoint = NJPoint(a.getY, a.getX)

      override def invert(b: NJPoint): Point = {
        val factory = new GeometryFactory()
        factory.createPoint(new Coordinate(b.longtitude, b.latitude))
      }
    }

  implicit val polygonInjection: Injection[Polygon, NJPolygon] = new Injection[Polygon, NJPolygon] {
    override def apply(a: Polygon): NJPolygon = NJPolygon(a.getCoordinates)

    override def invert(b: NJPolygon): Polygon = {
      val factory = new GeometryFactory()
      factory.createPolygon(b.value)
    }
  }
}
