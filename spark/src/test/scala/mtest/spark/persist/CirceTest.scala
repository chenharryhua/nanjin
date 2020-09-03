package mtest.spark.persist

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import io.circe.generic.auto._
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class CirceTest extends AnyFunSuite {
  import RoosterData._

  test("rdd read/write identity multi") {
    val path = "./data/test/spark/persist/circe/rooster/multi"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd)
    saver.circe(path).folder.run(blocker).unsafeRunSync()
    val t = loaders.circe[Rooster](path)
    assert(expected == t.collect().toSet)
  }

  test("rdd read/write identity") {
    val path = "./data/test/spark/persist/circe/rooster/single.json"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd)
    saver.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.circe[Rooster](path)
    assert(expected == t.collect().toSet)
  }
}
