package mtest.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.persist.{loaders, DatasetFileHoarder}
import kantan.csv.CsvConfiguration
import kantan.csv.generic.*
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class CsvTest extends AnyFunSuite {
  import TabletData.*

  val saver = new DatasetFileHoarder[IO, Tablet](ds)

  test("tablet read/write identity multi.uncompressed") {
    val path = "./data/test/spark/persist/csv/tablet/multi.uncompressed"
    saver.csv(path).folder.append.errorIfExists.ignoreIfExists.overwrite.uncompress.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity multi.gzip") {
    val path = "./data/test/spark/persist/csv/tablet/multi.gzip"
    saver.csv(path).folder.gzip.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity multi.1.deflate") {
    val path = "./data/test/spark/persist/csv/tablet/multi.1.deflate"
    saver.csv(path).folder.deflate(1).run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }
  test("tablet read/write identity multi.9.deflate") {
    val path = "./data/test/spark/persist/csv/tablet/multi.9.deflate"
    saver.csv(path).folder.deflate(9).run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity multi.bzip2") {
    val path = "./data/test/spark/persist/csv/tablet/bzip2.deflate"
    saver.csv(path).folder.bzip2.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity single.uncompressed") {
    val path = "./data/test/spark/persist/csv/tablet/tablet.csv"
    saver.csv(path).withoutHeader.quoteWhenNeeded.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity single.gzip") {
    val path = "./data/test/spark/persist/csv/tablet/tablet.csv.gz"
    saver.csv(path).file.gzip.sink.compile.drain.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity single.1.deflate") {
    val path = "./data/test/spark/persist/csv/tablet/tablet.1.csv.deflate"
    saver.csv(path).file.deflate(1).sink.compile.drain.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }
  test("tablet read/write identity single.9.deflate") {
    val path = "./data/test/spark/persist/csv/tablet/tablet.9.csv.deflate"
    saver.csv(path).file.deflate(9).sink.compile.drain.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header/single") {
    val path = "./data/test/spark/persist/csv/tablet/tablet_header.csv"
    val rfc  = CsvConfiguration.rfc.withHeader
    saver.csv(path).withHeader.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }
  test("tablet read/write identity with-header/multi") {
    val path = "./data/test/spark/persist/csv/tablet/tablet_header_multi.csv"
    val rfc  = CsvConfiguration.rfc.withHeader
    saver.csv(path).withHeader.folder.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header-delimiter/single") {
    val path = "./data/test/spark/persist/csv/tablet/tablet_header_delimit.csv"
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|')
    saver.csv(path).withHeader.withCellSeparator('|').file.sink.compile.drain.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header-delimiter/multi") {
    val path = "./data/test/spark/persist/csv/tablet/tablet_header_delimit_multi.csv"
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|')
    saver.csv(path).withHeader.withCellSeparator('|').folder.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header-delimiter-quote/single") {
    val path = "./data/test/spark/persist/csv/tablet/tablet_header_delimit_quote.csv"
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|').withQuote('*').quoteAll
    saver.csv(path).withHeader.withCellSeparator('|').withQuote('*').quoteAll.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header-delimiter-quote/multi") {
    val path = "./data/test/spark/persist/csv/tablet/tablet_header_delimit_quote_multi.csv"
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|').withQuote('*').quoteAll
    saver.csv(path).withHeader.withCellSeparator('|').withQuote('*').quoteAll.folder.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession).dataset
    assert(data.toSet == t.collect().toSet)
  }
}
