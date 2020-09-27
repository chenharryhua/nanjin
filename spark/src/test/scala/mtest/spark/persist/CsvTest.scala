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

  test("rdd read/write identity multi.uncompressed") {
    val path  = "./data/test/spark/persist/csv/tablet/multi.uncompressed"
    val saver = new RddFileHoarder[IO, Tablet](rdd, Tablet.codec)
    saver.csv(path).folder.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity multi.gzip") {
    val path  = "./data/test/spark/persist/csv/tablet/multi.gzip"
    val saver = new RddFileHoarder[IO, Tablet](rdd, Tablet.codec)
    saver.csv(path).folder.gzip.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity multi.deflate") {
    val path  = "./data/test/spark/persist/csv/tablet/multi.deflate"
    val saver = new RddFileHoarder[IO, Tablet](rdd, Tablet.codec)
    saver.csv(path).folder.deflate(3).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.uncompressed") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet.csv"
    val saver = new RddFileHoarder[IO, Tablet](rdd, Tablet.codec)
    saver.csv(path).file.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.gzip") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet.csv.gz"
    val saver = new RddFileHoarder[IO, Tablet](rdd, Tablet.codec)
    saver.csv(path).file.gzip.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.deflate") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet.csv.deflate"
    val saver = new RddFileHoarder[IO, Tablet](rdd, Tablet.codec)
    saver.csv(path).file.deflate(3).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
}
