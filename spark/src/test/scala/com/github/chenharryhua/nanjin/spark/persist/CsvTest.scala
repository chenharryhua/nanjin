package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import kantan.csv.CsvConfiguration
import kantan.csv.generic.*
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class CsvTest extends AnyFunSuite {
  import TabletData.*

  def saver(path: NJPath) = new DatasetFileHoarder[IO, Tablet](ds, HoarderConfig(path))

  test("tablet read/write identity multi.uncompressed") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/multi.uncompressed")
    saver(path).csv.folder.append.errorIfExists.ignoreIfExists.overwrite.uncompress.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity multi.gzip") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/multi.gzip")
    saver(path).csv.folder.gzip.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity multi.1.deflate") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/multi.1.deflate")
    saver(path).csv.folder.deflate(1).run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }
  test("tablet read/write identity multi.9.deflate") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/multi.9.deflate")
    saver(path).csv.folder.deflate(9).run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity multi.bzip2") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/bzip2.deflate")
    saver(path).csv.folder.bzip2.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity single.uncompressed") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet.csv")
    saver(path).csv.withoutHeader.quoteWhenNeeded.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity single.gzip") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet.csv.gz")
    saver(path).csv.file.gzip.sink.compile.drain.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity single.1.deflate") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet.1.csv.deflate")
    saver(path).csv.file.deflate(1).sink.compile.drain.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }
  test("tablet read/write identity single.9.deflate") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet.9.csv.deflate")
    saver(path).csv.file.deflate(9).sink.compile.drain.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header/single") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet_header.csv")
    val rfc  = CsvConfiguration.rfc.withHeader
    saver(path).csv.withHeader.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }
  test("tablet read/write identity with-header/multi") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet_header_multi.csv")
    val rfc  = CsvConfiguration.rfc.withHeader
    saver(path).csv.withHeader.folder.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header-delimiter/single") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet_header_delimit.csv")
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|')
    saver(path).csv.withHeader.withCellSeparator('|').file.sink.compile.drain.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header-delimiter/multi") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet_header_delimit_multi.csv")
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|')
    saver(path).csv.withHeader.withCellSeparator('|').folder.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header-delimiter-quote/single") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet_header_delimit_quote.csv")
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|').withQuote('*').quoteAll
    saver(path).csv.withHeader.withCellSeparator('|').withQuote('*').quoteAll.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header-delimiter-quote/multi") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet_header_delimit_quote_multi.csv")
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|').withQuote('*').quoteAll
    saver(path).csv.withHeader.withCellSeparator('|').withQuote('*').quoteAll.folder.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }
}
