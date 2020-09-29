package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.scalatest.funsuite.AnyFunSuite
import kantan.csv.generic._
import org.scalatest.DoNotDiscover

@DoNotDiscover
class CsvTest extends AnyFunSuite {
  import TabletData._

  test("tablet read/write identity multi.uncompressed") {
    val path  = "./data/test/spark/persist/csv/tablet/multi.uncompressed"
    val saver = new RddFileHoarder[IO, Tablet](rdd.repartition(1), Tablet.codec)
    saver.csv(path).folder.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity multi.gzip") {
    val path  = "./data/test/spark/persist/csv/tablet/multi.gzip"
    val saver = new RddFileHoarder[IO, Tablet](rdd.repartition(1), Tablet.codec)
    saver.csv(path).folder.gzip.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity multi.1.deflate") {
    val path  = "./data/test/spark/persist/csv/tablet/multi.1.deflate"
    val saver = new RddFileHoarder[IO, Tablet](rdd.repartition(1), Tablet.codec)
    saver.csv(path).folder.deflate(1).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
  test("tablet read/write identity multi.9.deflate") {
    val path  = "./data/test/spark/persist/csv/tablet/multi.9.deflate"
    val saver = new RddFileHoarder[IO, Tablet](rdd.repartition(1), Tablet.codec)
    saver.csv(path).folder.deflate(9).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity single.uncompressed") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet.csv"
    val saver = new RddFileHoarder[IO, Tablet](rdd, Tablet.codec)
    saver.csv(path).file.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity single.gzip") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet.csv.gz"
    val saver = new RddFileHoarder[IO, Tablet](rdd, Tablet.codec)
    saver.csv(path).file.gzip.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity single.1.deflate") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet.1.csv.deflate"
    val saver = new RddFileHoarder[IO, Tablet](rdd, Tablet.codec)
    saver.csv(path).file.deflate(1).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
  test("tablet read/write identity single.9.deflate") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet.9.csv.deflate"
    val saver = new RddFileHoarder[IO, Tablet](rdd, Tablet.codec)
    saver.csv(path).file.deflate(9).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
}
