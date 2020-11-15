package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class JacksonTest extends AnyFunSuite {
  test("datetime read/write identity - multi") {
    import RoosterData._
    val path  = "./data/test/spark/persist/jackson/rooster/multi.json"
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.jackson(path).folder.run(blocker).unsafeRunSync()
    val r = loaders.rdd.jackson[Rooster](path, Rooster.avroCodec)
    assert(expected == r.collect().toSet)
  }
  test("datetime read/write identity - single") {
    import RoosterData._
    val path  = "./data/test/spark/persist/jackson/rooster/single.json"
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.jackson(path).file.run(blocker).unsafeRunSync()
    val r = loaders.rdd.jackson[Rooster](path, Rooster.avroCodec)
    assert(expected == r.collect().toSet)
  }

  test("byte-array read/write identity - single") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/jackson/bee/single.json"
    val saver = new RddFileHoarder[IO, Bee](rdd, Bee.codec).repartition(1)
    saver.jackson(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.codec).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }
  test("byte-array read/write identity - multi") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/jackson/bee/multi.json"
    val saver = new RddFileHoarder[IO, Bee](rdd, Bee.codec).repartition(1)
    saver.jackson(path).folder.run(blocker).unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.codec).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }
  test("byte-array read/write identity - multi.gzip") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/jackson/bee/multi.gzip.json"
    val saver = new RddFileHoarder[IO, Bee](rdd, Bee.codec).repartition(1)
    saver.jackson(path).folder.gzip.run(blocker).unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.codec).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }
  test("byte-array read/write identity - multi.deflate") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/jackson/bee/multi.deflate.json"
    val saver = new RddFileHoarder[IO, Bee](rdd, Bee.codec).repartition(1)
    saver.jackson(path).folder.deflate(9).run(blocker).unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.codec).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - single.gzip") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/jackson/bee/single.json.gz"
    val saver = new RddFileHoarder[IO, Bee](rdd, Bee.codec).repartition(1)
    saver.jackson(path).file.gzip.run(blocker).unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.codec).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }
  test("byte-array read/write identity - single.deflate") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/jackson/bee/single.json.deflate"
    val saver = new RddFileHoarder[IO, Bee](rdd, Bee.codec).repartition(1)
    saver.jackson(path).file.deflate(3).run(blocker).unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.codec).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

}
