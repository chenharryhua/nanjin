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
    saver(path).csv.append.errorIfExists.ignoreIfExists.overwrite.uncompress.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity multi.gzip") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/multi.gzip")
    saver(path).csv.gzip.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity multi.1.deflate") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/multi.1.deflate")
    saver(path).csv.deflate(1).run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }
  test("tablet read/write identity multi.9.deflate") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/multi.9.deflate")
    saver(path).csv.deflate(9).run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity multi.bzip2") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/bzip2.deflate")
    saver(path).csv.bzip2.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header/multi") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet_header_multi.csv")
    val rfc  = CsvConfiguration.rfc.withHeader
    saver(path).csv.withHeader.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header-delimiter/multi") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet_header_delimit_multi.csv")
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|')
    saver(path).csv.withHeader.withCellSeparator('|').run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header-delimiter-quote/multi") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet_header_delimit_quote_multi.csv")
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|').withQuote('*').quoteAll
    saver(path).csv.withHeader.withCellSeparator('|').withQuote('*').quoteAll.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }
}
