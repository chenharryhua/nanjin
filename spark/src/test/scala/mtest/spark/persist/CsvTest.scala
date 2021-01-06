package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, DatasetFileHoarder}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import kantan.csv.CsvConfiguration
import kantan.csv.generic._
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import mtest.spark._

@DoNotDiscover
class CsvTest extends AnyFunSuite {
  import TabletData._

  val saver = new DatasetFileHoarder[IO, Tablet](ds)

  test("tablet read/write identity multi.uncompressed") {
    val path = "./data/test/spark/persist/csv/tablet/multi.uncompressed"
    saver.csv(path).errorIfExists.ignoreIfExists.overwrite.outPath(path).folder.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity multi.gzip") {
    val path = "./data/test/spark/persist/csv/tablet/multi.gzip"
    saver.csv(path).folder.gzip.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity multi.1.deflate") {
    val path = "./data/test/spark/persist/csv/tablet/multi.1.deflate"
    saver.csv(path).folder.deflate(1).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
  test("tablet read/write identity multi.9.deflate") {
    val path = "./data/test/spark/persist/csv/tablet/multi.9.deflate"
    saver.csv(path).folder.deflate(9).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity single.uncompressed") {
    val path = "./data/test/spark/persist/csv/tablet/tablet.csv"
    saver.csv(path).withoutHeader.quoteWhenNeeded.file.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity single.gzip") {
    val path = "./data/test/spark/persist/csv/tablet/tablet.csv.gz"
    saver.csv(path).file.gzip.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity single.1.deflate") {
    val path = "./data/test/spark/persist/csv/tablet/tablet.1.csv.deflate"
    saver.csv(path).file.deflate(1).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
  test("tablet read/write identity single.9.deflate") {
    val path = "./data/test/spark/persist/csv/tablet/tablet.9.csv.deflate"
    saver.csv(path).file.deflate(9).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity with-header/single") {
    val path = "./data/test/spark/persist/csv/tablet/tablet_header.csv"
    val rfc  = CsvConfiguration.rfc.withHeader
    saver.csv(path).file.withHeader.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
  test("tablet read/write identity with-header/multi") {
    val path = "./data/test/spark/persist/csv/tablet/tablet_header_multi.csv"
    val rfc  = CsvConfiguration.rfc.withHeader
    saver.csv(path).folder.withHeader.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity with-header-delimiter/single") {
    val path = "./data/test/spark/persist/csv/tablet/tablet_header_delimit.csv"
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|')
    saver.csv(path).file.withHeader.withCellSeparator('|').run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity with-header-delimiter/multi") {
    val path = "./data/test/spark/persist/csv/tablet/tablet_header_delimit_multi.csv"
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|')
    saver.csv(path).folder.withHeader.withCellSeparator('|').run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity with-header-delimiter-quote/single") {
    val path = "./data/test/spark/persist/csv/tablet/tablet_header_delimit_quote.csv"
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|').withQuote('*').quoteAll
    saver.csv(path).file.withHeader.withCellSeparator('|').withQuote('*').quoteAll.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity with-header-delimiter-quote/multi") {
    val path = "./data/test/spark/persist/csv/tablet/tablet_header_delimit_quote_multi.csv"
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|').withQuote('*').quoteAll
    saver.csv(path).folder.withHeader.withCellSeparator('|').withQuote('*').quoteAll.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
}
