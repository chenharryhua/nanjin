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

  def saver(path: NJPath, cfg: CsvConfiguration): SaveKantanCsv[IO, Tablet] =
    new RddFileHoarder[IO, Tablet](rdd).kantan(path, cfg)

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
    val cfg  = CsvConfiguration.rfc
    val s    = saver(path, cfg).uncompress
    s.run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
    //  val t3 = loaders.spark.csv(path, Tablet.ate, sparkSession).collect().toSet
    //  assert(data.toSet == t3)
  }

  test("tablet read/write identity multi.gzip") {
    val path = root / "gzip"
    val cfg  = CsvConfiguration.rfc
    val s    = saver(path, cfg).gzip
    s.run.unsafeRunSync()

    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
  }

  test("tablet read/write identity multi.1.deflate") {
    val path = root / "deflate1"
    val cfg  = CsvConfiguration.rfc
    val s    = saver(path, cfg).deflate(1)
    s.run.unsafeRunSync()
    val t  = loaders.rdd.kantan[Tablet](path, sparkSession, cfg).collect().toSet
    val t2 = loadTablet(path, cfg).unsafeRunSync()

    assert(data.toSet == t)
    assert(data.toSet == t2)
  }

  test("tablet read/write identity multi.9.deflate") {
    val path = root / "deflate9"
    val cfg  = CsvConfiguration.rfc
    val s    = saver(path, cfg)
    s.deflate(9).run.unsafeRunSync()

    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
  }

  test("tablet read/write identity multi.bzip2") {
    val path = root / "bzip2"
    val cfg  = CsvConfiguration.rfc.withHeader
    val s    = saver(path, cfg).bzip2
    s.run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
  }

  test("tablet read/write identity multi.lz4") {
    val path = root / "lz4"
    val cfg  = CsvConfiguration.rfc.withHeader
    val s    = saver(path, cfg).lz4
    s.run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
  }

//  test("tablet read/write identity multi.snappy") {
//    val path = root / "snappy"
//    val s    = saver(path).snappy.withHeader
//    s.run.unsafeRunSync()
//    val t = loaders.rdd.kantan[Tablet](path, s.csvConfiguration, sparkSession)
//    assert(data.toSet == t.collect().toSet)
//    assert(data.toSet == loadTablet(path, s.csvConfiguration).unsafeRunSync())
//  }

  test("tablet read/write identity with-explicit-header") {
    val path = root / "header_explicit"
    val cfg  = CsvConfiguration.rfc.withHeader("x", "y", "z")
    val s    = saver(path, cfg)
    s.run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
  }

  test("tablet read/write identity with-implicit-header") {
    val path = root / "header_implicit"
    val cfg  = CsvConfiguration.rfc.withHeader
    val s    = saver(path, cfg)
    s.run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
  }

  test("tablet read/write identity with-header-delimiter") {
    val path = root / "header_delimiter"
    val cfg  = CsvConfiguration.rfc.withCellSeparator('|').withHeader
    val s    = saver(path, cfg)
    s.run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
  }

  test("tablet read/write identity with-header-delimiter-quote") {
    val path = root / "header_delimiter_quote"
    val cfg  = CsvConfiguration.rfc.withHeader.withCellSeparator('|').withQuote('*').quoteAll
    val s    = saver(path, cfg)
    s.run.unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
  }
}
