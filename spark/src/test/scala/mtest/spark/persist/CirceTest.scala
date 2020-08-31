package mtest.spark.persist

import cats.implicits._
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.persist.{loaders, savers}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import io.circe.generic.auto._
import org.scalatest.funsuite.AnyFunSuite

class CirceTest extends AnyFunSuite {
  import RoosterData._

  test("rdd read/write identity") {
    val path = "./data/test/spark/persist/circe"
    delete(path)
    savers.circe(rdd, path)
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
  }
}
