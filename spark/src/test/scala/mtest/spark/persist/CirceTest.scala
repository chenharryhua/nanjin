package mtest.spark.persist

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileSaver}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import io.circe.generic.auto._
import org.scalatest.funsuite.AnyFunSuite

class CirceTest extends AnyFunSuite {
  import RoosterData._

  test("rdd read/write identity") {
    val path = "./data/test/spark/persist/circe/rooster"
    delete(path)
    val saver = new RddFileSaver[IO, Rooster](rdd)
    saver.circe(path).run(blocker).unsafeRunSync()
    val t = loaders.circe[Rooster](path)
    assert(expected == t.collect().toSet)
    println(Bee.avroDecoder.schema)
  }
}
