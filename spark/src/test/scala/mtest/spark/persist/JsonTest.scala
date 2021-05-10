package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, DatasetAvroFileHoarder, DatasetFileHoarder}
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import mtest.spark._
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import cats.effect.unsafe.implicits.global

@DoNotDiscover
class JsonTest extends AnyFunSuite {

  val rooster =
    new DatasetAvroFileHoarder[IO, Rooster](RoosterData.ds, Rooster.avroCodec.avroEncoder)

  test("rdd read/write identity uncompressed - keep null") {
    import RoosterData._
    val path = "./data/test/spark/persist/json/uncompressed.keepNull.json"
    rooster.json(path).errorIfExists.ignoreIfExists.overwrite.keepNull.uncompress.run.unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity uncompressed - drop null") {
    import RoosterData._
    val path = "./data/test/spark/persist/json/uncompressed.dropNull.json"
    rooster.json(path).dropNull.run.unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity gzip") {
    import RoosterData._
    val path = "./data/test/spark/persist/json/gzip.json"
    rooster.json(path).gzip.run.unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity deflate") {
    import RoosterData._
    val path = "./data/test/spark/persist/json/deflate.json"
    rooster.json(path).deflate(1).run.unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
  }
  test("rdd read/write identity bzip2") {
    import RoosterData._
    val path = "./data/test/spark/persist/json/bzip2.json"
    rooster.json(path).bzip2.run.unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json(path, Rooster.ate, sparkSession)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
  }
  test("json jacket") {
    import JacketData._
    val path  = "./data/test/spark/persist/json/jacket.json"
    val saver = new DatasetFileHoarder[IO, Jacket](ds)
    saver.json(path).run.unsafeRunSync()
    val t: TypedDataset[Jacket] = loaders.json(path, Jacket.ate, sparkSession)
    assert(expected.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

}
