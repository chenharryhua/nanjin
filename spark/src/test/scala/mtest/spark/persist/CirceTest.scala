package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import io.circe.generic.auto._
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class CirceTest extends AnyFunSuite {

  test("rdd read/write identity multi.gzip") {
    import RoosterData._
    val path  = "./data/test/spark/persist/circe/rooster/multi.gzip"
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec).repartition(1)
    saver.circe(path).folder.gzip.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity multi.deflate") {
    import RoosterData._
    val path  = "./data/test/spark/persist/circe/rooster/multi.deflate"
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.circe(path).folder.deflate(3).run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.uncompressed") {
    import RoosterData._
    val path  = "./data/test/spark/persist/circe/rooster/single.json"
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.gzip") {
    import RoosterData._
    val path  = "./data/test/spark/persist/circe/rooster/single.json.gz"
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.circe(path).file.gzip.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.deflate") {
    import RoosterData._
    val path  = "./data/test/spark/persist/circe/rooster/single.json.deflate"
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.circe(path).file.deflate(3).run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }
  test("byte-array rdd read/write identity multi") {
    import BeeData._
    val path  = "./data/test/spark/persist/circe/bee/multi.json"
    val saver = new RddFileHoarder[IO, Bee](rdd, Bee.codec).repartition(1)
    saver.circe(path).folder.dropNull.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path)
    assert(t.collect.map(_.toWasp).toSet === bees.map(_.toWasp).toSet)
  }
  test("byte-array rdd read/write identity single") {
    import BeeData._
    val path  = "./data/test/spark/persist/circe/bee/single.json"
    val saver = new RddFileHoarder[IO, Bee](rdd, Bee.codec).repartition(1)
    saver.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path)
    assert(t.collect.map(_.toWasp).toSet === bees.map(_.toWasp).toSet)
  }

  test("rdd read/write identity multi.uncompressed - keep null") {
    import RoosterData._
    val path  = "./data/test/spark/persist/circe/rooster/multi.keepNull.uncompressed"
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.repartition(1).circe(path).folder.keepNull.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity multi.uncompressed - drop null") {
    import RoosterData._
    val path  = "./data/test/spark/persist/circe/rooster/multi.dropNull.uncompressed"
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.repartition(1).circe(path).folder.dropNull.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }
  test("rdd read/write identity single.uncompressed - keep null") {
    import RoosterData._
    val path  = "./data/test/spark/persist/circe/rooster/single.keepNull.uncompressed.json"
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.repartition(1).circe(path).file.keepNull.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.uncompressed - drop null") {
    import RoosterData._
    val path  = "./data/test/spark/persist/circe/rooster/single.dropNull.uncompressed.json"
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.repartition(1).circe(path).file.dropNull.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }
  test("circe jacket") {
    import JacketData._
    val path  = "./data/test/spark/persist/circe/jacket.json"
    val saver = new RddFileHoarder[IO, Jacket](rdd, Jacket.avroCodec)
    saver.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Jacket](path).collect().toSet
    assert(expected.toSet == t)
    val t2 = loaders.circe[Jacket](path, Jacket.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected.toSet == t2)
  }
}
