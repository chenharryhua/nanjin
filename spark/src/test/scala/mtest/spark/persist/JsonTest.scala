package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class JsonTest extends AnyFunSuite {

  test("rdd read/write identity uncompressed - keep null") {
    import RoosterData._
    val path  = "./data/test/spark/persist/json/uncompressed.keepNull.json"
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.repartition(1).json(path).keepNull.run(blocker).unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
    val t2: TypedDataset[Rooster] = loaders.circe[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity uncompressed - drop null") {
    import RoosterData._
    val path  = "./data/test/spark/persist/json/uncompressed.dropNull.json"
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.repartition(1).json(path).dropNull.run(blocker).unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
    val t2: TypedDataset[Rooster] = loaders.circe[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity gzip") {
    import RoosterData._
    val path  = "./data/test/spark/persist/json/gzip.json"
    val saver = new RddFileHoarder[IO, Rooster](rdd.repartition(1), Rooster.avroCodec)
    saver.json(path).gzip.run(blocker).unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
    val t2: TypedDataset[Rooster] = loaders.circe[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity deflate") {
    import RoosterData._
    val path  = "./data/test/spark/persist/json/deflate.json"
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.json(path).deflate(1).run(blocker).unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
    val t2: TypedDataset[Rooster] = loaders.circe[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }
  test("rdd read/write identity bzip2") {
    import RoosterData._
    val path  = "./data/test/spark/persist/json/bzip2.json"
    val saver = new RddFileHoarder[IO, Rooster](rdd.repartition(1), Rooster.avroCodec)
    saver.json(path).bzip2.run(blocker).unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json(path, Rooster.ate)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
    val t2: TypedDataset[Rooster] = loaders.circe[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }
  test("json jacket") {
    import JacketData._
    val path  = "./data/test/spark/persist/json/jacket.json"
    val saver = new RddFileHoarder[IO, Jacket](rdd.repartition(1), Jacket.avroCodec)
    saver.json(path).run(blocker).unsafeRunSync()
    val t: TypedDataset[Jacket] = loaders.json(path, Jacket.ate)
    assert(expected.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
}
