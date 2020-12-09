package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.spark.persist.DatasetFileHoarder

@DoNotDiscover
class JsonTest extends AnyFunSuite {

  test("rdd read/write identity uncompressed - keep null") {
    import RoosterData._
    val path  = "./data/test/spark/persist/json/uncompressed.keepNull.json"
    val saver = DatasetFileHoarder[IO, Rooster](ds.repartition(1), path)
    saver.json.keepNull.run(blocker).unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
    val t2: TypedDataset[Rooster] = loaders.circe[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity uncompressed - drop null") {
    import RoosterData._
    val path  = "./data/test/spark/persist/json/uncompressed.dropNull.json"
    val saver = DatasetFileHoarder[IO, Rooster](ds, path)
    saver.json.dropNull.run(blocker).unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
    val t2: TypedDataset[Rooster] = loaders.circe[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity gzip") {
    import RoosterData._
    val path  = "./data/test/spark/persist/json/gzip.json"
    val saver = DatasetFileHoarder[IO, Rooster](ds, path)
    saver.json.gzip.run(blocker).unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
    val t2: TypedDataset[Rooster] = loaders.circe[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity deflate") {
    import RoosterData._
    val path  = "./data/test/spark/persist/json/deflate.json"
    val saver = DatasetFileHoarder[IO, Rooster](ds, path)
    saver.json.deflate(1).run(blocker).unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
    val t2: TypedDataset[Rooster] = loaders.circe[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }
  test("rdd read/write identity bzip2") {
    import RoosterData._
    val path  = "./data/test/spark/persist/json/bzip2.json"
    val saver = DatasetFileHoarder[IO, Rooster](ds, path)
    saver.json.bzip2.run(blocker).unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json(path, Rooster.ate)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
    val t2: TypedDataset[Rooster] = loaders.circe[Rooster](path, Rooster.ate)
    assert(expected == t2.collect[IO]().unsafeRunSync().toSet)
  }
  test("json jacket") {
    import JacketData._
    val path  = "./data/test/spark/persist/json/jacket.json"
    val saver = DatasetFileHoarder[IO, Jacket](ds, path)
    saver.json.run(blocker).unsafeRunSync()
    val t: TypedDataset[Jacket] = loaders.json(path, Jacket.ate)
    assert(expected.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
}
