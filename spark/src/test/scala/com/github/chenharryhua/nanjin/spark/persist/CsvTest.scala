package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.CsvSerde
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.github.chenharryhua.nanjin.spark.*
import eu.timepit.refined.auto.*
import kantan.csv.CsvConfiguration
import kantan.csv.generic.*
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import fs2.Stream
import kantan.csv.java8.*

@DoNotDiscover
class CsvTest extends AnyFunSuite {
  import TabletData.*

  def saver(path: NJPath) = new DatasetFileHoarder[IO, Tablet](ds, HoarderConfig(path)).csv
  val hdp                 = sparkSession.hadoop[IO]
  def loadTablet(path: NJPath, cfg: CsvConfiguration) = Stream
    .force(
      hdp
        .inputFilesByName(path)
        .map(_.foldLeft(Stream.empty.covaryAll[IO, Tablet]) { case (ss, hip) =>
          ss ++ hdp.bytes.source(hip).through(CsvSerde.deserPipe[IO, Tablet](cfg, 100))
        }))
    .compile
    .toList
    .map(_.toSet)

  test("tablet read/write identity multi.uncompressed") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/multi.uncompressed")
    val s    = saver(path)
    s.append.errorIfExists.ignoreIfExists.overwrite.uncompress.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
    // assert(data.toSet == loadTablet(path, s.csvConfiguration).unsafeRunSync())
  }

  test("tablet read/write identity multi.gzip") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/multi.gzip")
    val s    = saver(path)
    s.gzip.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
    // assert(data.toSet == loadTablet(path, s.csvConfiguration).unsafeRunSync())
  }

  test("tablet read/write identity multi.1.deflate") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/multi.1.deflate")
    val s    = saver(path)
    s.deflate(1).run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
    //  assert(data.toSet == loadTablet(path, s.csvConfiguration).unsafeRunSync())
  }

  test("tablet read/write identity multi.9.deflate") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/multi.9.deflate")
    saver(path).deflate(9).run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity multi.bzip2") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/bzip2.deflate")
    saver(path).bzip2.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header/multi") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet_header_multi.csv")
    val rfc  = CsvConfiguration.rfc.withHeader
    saver(path).withHeader.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header-delimiter/multi") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet_header_delimit_multi.csv")
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|')
    saver(path).withHeader.withCellSeparator('|').run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }

  test("tablet read/write identity with-header-delimiter-quote/multi") {
    val path = NJPath("./data/test/spark/persist/csv/tablet/tablet_header_delimit_quote_multi.csv")
    val rfc  = CsvConfiguration.rfc.withHeader.withCellSeparator('|').withQuote('*').quoteAll
    saver(path).withHeader.withCellSeparator('|').withQuote('*').quoteAll.run.unsafeRunSync()
    val t = loaders.csv(path, Tablet.ate, rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
  }
}
