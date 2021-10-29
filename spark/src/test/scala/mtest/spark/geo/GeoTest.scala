package mtest.spark.geo
import com.github.chenharryhua.nanjin.spark.geo.*
import com.github.chenharryhua.nanjin.spark.geo.instances.*
import frameless.TypedDataset
import mtest.spark.sparkSession
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, Polygon}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

object GeoTestData {
  val c1 = new Coordinate(0, 0)
  val c2 = new Coordinate(0, 1)
  val c3 = new Coordinate(1, 0)
  val c4 = new Coordinate(1, 1)
  val c5 = new Coordinate(0, 0)

  val c6 = new Coordinate(0.2, 1.5)

  val gf: GeometryFactory = new GeometryFactory()

  val coordinates: List[Coordinate]  = List(c1, c2, c3, c4, c5)
  val coordinates2: List[Coordinate] = List(c1, c6, c2, c3, c4, c5)

  val polygon1: Polygon = gf.createPolygon(coordinates.toArray)
  val polygon2: Polygon = gf.createPolygon(coordinates2.toArray)
}

class GeoTest extends AnyFunSuite {
  import GeoTestData._
  implicit val ss: SparkSession = sparkSession
  test("return polygen if point is inside its boundary") {
    val pc = PolygonCollection[Polygon](List(polygon1), identity)
    assert(pc.find(gf.createPoint(new Coordinate(0.5, 0.5))).isDefined)
    assert(pc.find(gf.createPoint(new Coordinate(1.5, 0.5))).isEmpty)
  }

  test("covers") {
    val ds1: TypedDataset[Polygon] = TypedDataset.create(List(polygon1))
    val ds2: TypedDataset[Point] =
      TypedDataset.create(List(gf.createPoint(new Coordinate(0.1, 0.1)), gf.createPoint(new Coordinate(1.1, 1.1))))
    val joined: TypedDataset[(Polygon, Point)] = ds1.joinCross(ds2)

    val rst = joined.filter(covers(joined(Symbol("_1")), joined(Symbol("_2"))))
    assert(rst.dataset.count() == 1)
  }

  test("intersects") {
    val ds1: TypedDataset[Polygon] = TypedDataset.create(List(polygon1))
    val ds2: TypedDataset[Polygon] = TypedDataset.create(List(polygon2))
    val joined                     = ds1.joinCross(ds2)
    val rst                        = joined.filter(intersects(joined(Symbol("_1")), joined(Symbol("_2"))))
    assert(rst.dataset.count() == 1)
  }
}
