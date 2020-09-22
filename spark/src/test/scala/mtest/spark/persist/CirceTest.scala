package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import io.circe.generic.auto._
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class CirceTest extends AnyFunSuite {

  test("rdd read/write identity multi.uncompressed") {
    import RoosterData._
    val path = "./data/test/spark/persist/circe/rooster/multi.uncompressed"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.circe(path).folder.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
  }

  test("rdd read/write identity multi.gzip") {
    import RoosterData._
    val path = "./data/test/spark/persist/circe/rooster/multi.gzip"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.circe(path).folder.gzip.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
  }

  test("rdd read/write identity multi.deflate") {
    import RoosterData._
    val path = "./data/test/spark/persist/circe/rooster/multi.deflate"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.circe(path).folder.deflate(3).run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
  }

  test("rdd read/write identity single.uncompressed") {
    import RoosterData._
    val path = "./data/test/spark/persist/circe/rooster/single.json"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
  }

  test("rdd read/write identity single.gzip") {
    import RoosterData._
    val path = "./data/test/spark/persist/circe/rooster/single.json.gz"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.circe(path).file.gzip.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
  }

  test("rdd read/write identity single.deflate") {
    import RoosterData._
    val path = "./data/test/spark/persist/circe/rooster/single.json.deflate"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.circe(path).file.deflate(3).run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
  }

  test("byte-array rdd read/write identity bee") {
    import BeeData._
    val path = "./data/test/spark/persist/circe/bee/single.json"
    delete(path)
    val saver = new RddFileHoarder[IO, Bee](rdd, Bee.codec)
    saver.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path)
    assert(t.collect.map(_.toWasp).toSet === bees.map(_.toWasp).toSet)
  }
}
