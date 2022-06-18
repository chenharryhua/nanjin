package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.KantanSerde
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import fs2.Stream
import kantan.csv.generic.*
import kantan.csv.java8.*
import kantan.csv.{CsvConfiguration, HeaderEncoder, RowDecoder}
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class KantanCsvTest extends AnyFunSuite {
  import TabletData.*

  implicit val encoderTablet: HeaderEncoder[Tablet] = shapeless.cachedImplicit
  implicit val decoderTablet: RowDecoder[Tablet]    = shapeless.cachedImplicit

  def saver(path: NJPath) = new RddFileHoarder[IO, Tablet](rdd).kantan(path)

  val hdp = sparkSession.hadoop[IO]

  def loadTablet(path: NJPath, cfg: CsvConfiguration) = Stream
    .force(
      hdp
        .filesSortByName(path)
        .map(_.foldLeft(Stream.empty.covaryAll[IO, Tablet]) { case (ss, hip) =>
          ss ++ hdp.bytes.source(hip).through(KantanSerde.fromBytes[IO, Tablet](cfg))
        }))
    .compile
    .toList
    .map(_.toSet)

  val root = NJPath("./data/test/spark/persist/csv/tablet")
  test("tablet read/write identity multi.uncompressed") {
    val path = root / "uncompressed"
    val s    = saver(path).uncompress
    s.run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, s.csvConfiguration, sparkSession)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, s.csvConfiguration).unsafeRunSync())
    //  val t3 = loaders.spark.csv(path, Tablet.ate, sparkSession).collect().toSet
    //  assert(data.toSet == t3)
  }

  test("tablet read/write identity multi.gzip") {
    val path = root / "gzip"
    val s    = saver(path).gzip
    s.run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, CsvConfiguration.rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, s.csvConfiguration).unsafeRunSync())
  }

  test("tablet read/write identity multi.1.deflate") {
    val path = root / "deflate1"
    val s    = saver(path).deflate(1)
    s.run.unsafeRunSync()
    val t  = loaders.rdd.kantan[Tablet](path, CsvConfiguration.rfc, sparkSession).collect().toSet
    val t2 = loadTablet(path, s.csvConfiguration).unsafeRunSync()

    assert(data.toSet == t)
    assert(data.toSet == t2)
  }

  test("tablet read/write identity multi.9.deflate") {
    val path = root / "deflate9"
    val s    = saver(path)
    s.deflate(9).run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, CsvConfiguration.rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, s.csvConfiguration).unsafeRunSync())
  }

  test("tablet read/write identity multi.bzip2") {
    val path = root / "bzip2"
    val s    = saver(path).bzip2.withHeader
    s.run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, s.csvConfiguration, sparkSession)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, s.csvConfiguration).unsafeRunSync())
  }

  test("tablet read/write identity multi.lz4") {
    val path = root / "lz4"
    val s    = saver(path).lz4.withHeader
    s.run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, s.csvConfiguration, sparkSession)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, s.csvConfiguration).unsafeRunSync())
  }

//  test("tablet read/write identity multi.snappy") {
//    val path = root / "snappy"
//    val s    = saver(path).snappy.withHeader
//    s.run.unsafeRunSync()
//    val t = loaders.rdd.kantan[Tablet](path, s.csvConfiguration, sparkSession)
//    assert(data.toSet == t.collect().toSet)
//    assert(data.toSet == loadTablet(path, s.csvConfiguration).unsafeRunSync())
//  }

  test("tablet read/write identity with-header") {
    val path = root / "with_header"
    val s    = saver(path).withHeader("x", "y", "z")
    s.run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, s.csvConfiguration, sparkSession)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, s.csvConfiguration).unsafeRunSync())
  }

  test("tablet read/write identity with-header-delimiter") {
    val path = root / "with_pipe"
    val rfc  = CsvConfiguration.rfc.withCellSeparator('|')
    val s    = saver(path).updateCsvConfig(_ => rfc)
    s.run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, rfc, sparkSession)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, s.csvConfiguration).unsafeRunSync())
  }

  test("tablet read/write identity with-header-delimiter-quote") {
    val path = root / "with_quote"
    val s    = saver(path).withHeader.withCellSeparator('|').withQuote('*').quoteAll
    s.run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, s.csvConfiguration, sparkSession)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, s.csvConfiguration).unsafeRunSync())
  }
}
