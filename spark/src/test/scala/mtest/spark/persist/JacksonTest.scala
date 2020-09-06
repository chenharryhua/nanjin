package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class JacksonTest extends AnyFunSuite {
  test("datetime read/write identity multi") {
    import RoosterData._
    val path = "./data/test/spark/persist/jackson/multi.json"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd)
    saver.jackson(path).folder.run(blocker).unsafeRunSync()
    val r = loaders.raw.jackson[Rooster](path)
    assert(expected == r.collect().toSet)
  }
  test("datetime read/write identity single") {
    import RoosterData._
    val path = "./data/test/spark/persist/jackson/single.json"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd)
    saver.jackson(path).file.run(blocker).unsafeRunSync()
    val r = loaders.raw.jackson[Rooster](path)
    assert(expected == r.collect().toSet)
  }

  test("byte-array read/write identity single read") {
    import BeeData._
    import cats.implicits._
    val path = "./data/test/spark/persist/jackson/bee/single.raw.avro"
    delete(path)
    val saver = new RddFileHoarder[IO, Bee](rdd).repartition(1)
    saver.jackson(path).folder.run(blocker).unsafeRunSync()
    val t = loaders.raw.jackson[Bee](path).collect().toList
    println(t)
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }
}
