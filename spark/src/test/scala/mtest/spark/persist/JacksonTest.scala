package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.TypedDataset
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.spark.persist.AvroFileHoarder

@DoNotDiscover
class JacksonTest extends AnyFunSuite {
  test("datetime read/write identity - multi") {
    import RoosterData._
    val path = "./data/test/spark/persist/jackson/rooster/multi.json"
    val saver =
      AvroFileHoarder[IO, Rooster](rdd.repartition(3), path, Rooster.avroCodec.avroEncoder)
    saver.jackson.folder.run(blocker).unsafeRunSync()
    val r = loaders.rdd.jackson[Rooster](path, Rooster.avroCodec)
    assert(expected == r.collect().toSet)
  }

  test("datetime read/write identity - single") {
    import RoosterData._
    val path = "./data/test/spark/persist/jackson/rooster/single.json"
    val saver =
      AvroFileHoarder[IO, Rooster](rdd.repartition(3), path, Rooster.avroCodec.avroEncoder)
    saver.jackson.file.run(blocker).unsafeRunSync()
    val r = loaders.rdd.jackson[Rooster](path, Rooster.avroCodec)
    assert(expected == r.collect().toSet)
  }

  test("byte-array read/write identity - single") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/jackson/bee/single.json"
    val saver = AvroFileHoarder[IO, Bee](rdd.repartition(3), path, Bee.avroCodec.avroEncoder)
    saver.jackson.file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.avroCodec).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - multi") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/jackson/bee/multi.json"
    val saver = AvroFileHoarder[IO, Bee](rdd.repartition(3), path, Bee.avroCodec.avroEncoder)
    saver.jackson.folder.run(blocker).unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.avroCodec).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - multi.gzip") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/jackson/bee/multi.gzip.json"
    val saver = AvroFileHoarder[IO, Bee](rdd.repartition(3), path, Bee.avroCodec.avroEncoder)
    saver.jackson.folder.gzip.run(blocker).unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.avroCodec).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - multi.deflate") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/jackson/bee/multi.deflate.json"
    val saver = AvroFileHoarder[IO, Bee](rdd.repartition(3), path, Bee.avroCodec.avroEncoder)
    saver.jackson.folder.deflate(9).run(blocker).unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.avroCodec).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - single.gzip") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/jackson/bee/single.json.gz"
    val saver = AvroFileHoarder[IO, Bee](rdd.repartition(3), path, Bee.avroCodec.avroEncoder)
    saver.jackson.file.gzip.run(blocker).unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.avroCodec).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - single.deflate") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/jackson/bee/single.json.deflate"
    val saver = AvroFileHoarder[IO, Bee](rdd.repartition(3), path, Bee.avroCodec.avroEncoder)
    saver.jackson.file.deflate(3).run(blocker).unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.avroCodec).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("jackson jacket") {
    import JacketData._
    val path  = "./data/test/spark/persist/jackson/jacket.json"
    val saver = AvroFileHoarder[IO, Jacket](rdd.repartition(3), path, Jacket.avroCodec.avroEncoder)
    saver.jackson.run(blocker).unsafeRunSync()
    val t = loaders.rdd.jackson(path, Jacket.avroCodec)
    assert(expected.toSet == t.collect().toSet)
  }

  test("jackson fractual") {
    import FractualData._
    val path = "./data/test/spark/persist/jackson/fractual.json"
    val saver =
      AvroFileHoarder[IO, Fractual](rdd.repartition(3), path, Fractual.avroCodec.avroEncoder)
    saver.jackson.file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.jackson[Fractual](path, Fractual.avroCodec).collect().toSet
    assert(data.toSet == t)
  }
}
