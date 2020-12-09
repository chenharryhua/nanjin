package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import kantan.csv.CsvConfiguration
import kantan.csv.generic._
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.spark.persist.DatasetFileHoarder

@DoNotDiscover
class CsvTest extends AnyFunSuite {
  import TabletData._

  test("tablet read/write identity multi.uncompressed") {
    val path  = "./data/test/spark/persist/csv/tablet/multi.uncompressed"
    val saver =  DatasetFileHoarder[IO, Tablet](ds, path)
    saver.csv.folder.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity multi.gzip") {
    val path  = "./data/test/spark/persist/csv/tablet/multi.gzip"
    val saver =  DatasetFileHoarder[IO, Tablet](ds, path)
    saver.csv.folder.gzip.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity multi.1.deflate") {
    val path  = "./data/test/spark/persist/csv/tablet/multi.1.deflate"
    val saver =  DatasetFileHoarder[IO, Tablet](ds, path)
    saver.csv.folder.deflate(1).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
  test("tablet read/write identity multi.9.deflate") {
    val path  = "./data/test/spark/persist/csv/tablet/multi.9.deflate"
    val saver =  DatasetFileHoarder[IO, Tablet](ds, path)
    saver.csv.folder.deflate(9).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity single.uncompressed") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet.csv"
    val saver =  DatasetFileHoarder[IO, Tablet](ds, path)
    saver.csv.file.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity single.gzip") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet.csv.gz"
    val saver =  DatasetFileHoarder[IO, Tablet](ds, path)
    saver.csv.file.gzip.run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity single.1.deflate") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet.1.csv.deflate"
    val saver =  DatasetFileHoarder[IO, Tablet](ds, path)
    saver.csv.file.deflate(1).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
  test("tablet read/write identity single.9.deflate") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet.9.csv.deflate"
    val saver =  DatasetFileHoarder[IO, Tablet](ds, path)
    saver.csv.file.deflate(9).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity with-header/single") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet_header.csv"
    val saver =  DatasetFileHoarder[IO, Tablet](ds, path)
    val rfc   = CsvConfiguration.rfc.withHeader
    saver.csv.file.updateCsvConfig(_ => rfc).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
  test("tablet read/write identity with-header/multi") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet_header_multi.csv"
    val saver =  DatasetFileHoarder[IO, Tablet](ds, path)
    val rfc   = CsvConfiguration.rfc.withHeader
    saver.csv.folder.updateCsvConfig(_ => rfc).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity with-header-delimiter/single") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet_header_delimit.csv"
    val saver =  DatasetFileHoarder[IO, Tablet](ds, path)
    val rfc   = CsvConfiguration.rfc.withHeader.withCellSeparator('|')
    saver.csv.file.updateCsvConfig(_ => rfc).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity with-header-delimiter/multi") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet_header_delimit_multi.csv"
    val saver =  DatasetFileHoarder[IO, Tablet](ds, path)
    val rfc   = CsvConfiguration.rfc.withHeader.withCellSeparator('|')
    saver.csv.folder.updateCsvConfig(_ => rfc).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity with-header-delimiter-quote/single") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet_header_delimit_quote.csv"
    val saver =  DatasetFileHoarder[IO, Tablet](ds, path)
    val rfc   = CsvConfiguration.rfc.withHeader.withCellSeparator('|').withQuote('*').quoteAll
    saver.csv.file.updateCsvConfig(_ => rfc).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("tablet read/write identity with-header-delimiter-quote/multi") {
    val path  = "./data/test/spark/persist/csv/tablet/tablet_header_delimit_quote_multi.csv"
    val saver =  DatasetFileHoarder[IO, Tablet](ds, path)
    val rfc   = CsvConfiguration.rfc.withHeader.withCellSeparator('|').withQuote('*').quoteAll
    saver.csv.folder.updateCsvConfig(_ => rfc).run(blocker).unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc)
    assert(data.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
}
