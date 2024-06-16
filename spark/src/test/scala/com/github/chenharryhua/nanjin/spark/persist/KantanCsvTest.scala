package com.github.chenharryhua.nanjin.spark.persist

import better.files.File
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath, csvHeader}
import eu.timepit.refined.auto.*
import kantan.csv.generic.*
import kantan.csv.java8.*
import kantan.csv.{CsvConfiguration, RowDecoder, RowEncoder}
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class KantanCsvTest extends AnyFunSuite {
  import TabletData.*

  implicit val encoderTablet: RowEncoder[Tablet] = shapeless.cachedImplicit
  implicit val decoderTablet: RowDecoder[Tablet] = shapeless.cachedImplicit

  def saver(path: NJPath, cfg: CsvConfiguration): SaveKantanCsv[Tablet] =
    new RddFileHoarder[Tablet](rdd).kantan(path, cfg)

  val hdp: NJHadoop[IO] = sparkSession.hadoop[IO]

  def loadTablet(path: NJPath, cfg: CsvConfiguration): IO[Set[Tablet]] = {
    val kantan = hdp.kantan(cfg)
    hdp
      .filesIn(path)
      .flatMap(_.flatTraverse(kantan.source(_, 100).map(decoderTablet.decode).rethrow.compile.toList))
      .map(_.toSet)
  }

  val root: NJPath = NJPath("./data/test/spark/persist/csv/tablet")
  test("1.tablet read/write identity multi.uncompressed") {
    val path = root / "uncompressed"
    val cfg  = CsvConfiguration.rfc
    val s    = saver(path, cfg).withCompression(_.Uncompressed)
    s.run[IO].unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
    //  val t3 = loaders.spark.csv(path, Tablet.ate, sparkSession).collect().toSet
    //  assert(data.toSet == t3)
  }

  test("2.tablet read/write identity multi.gzip") {
    val path = root / "gzip"
    val cfg  = CsvConfiguration.rfc
    val s    = saver(path, cfg).withCompression(_.Gzip)
    s.run[IO].unsafeRunSync()

    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
  }

  test("3.tablet read/write identity multi.9.deflate") {
    val path = root / "deflate9"
    val cfg  = CsvConfiguration.rfc
    val s    = saver(path, cfg)
    s.withCompression(_.Deflate(9)).run[IO].unsafeRunSync()

    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
  }

  test("4.tablet read/write identity multi.bzip2") {
    val path = root / "bzip2"
    val cfg  = CsvConfiguration.rfc.withHeader
    val s    = saver(path, cfg).withCompression(_.Bzip2)
    s.run[IO].unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
  }

  test("5.tablet read/write identity multi.lz4") {
    val path = root / "lz4"
    val cfg  = CsvConfiguration.rfc.withHeader
    val s    = saver(path, cfg).withCompression(_.Lz4)
    s.run[IO].unsafeRunSync()
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

  def checkHeader(path: NJPath, header: String): Unit =
    File(path.pathStr)
      .list(_.extension.contains(".csv"))
      .map(_.lineIterator.toList.head === header)
      .foreach(assert(_))

  test("6.tablet read/write identity with-explicit-header") {
    val path = root / "header_explicit"
    val cfg  = CsvConfiguration.rfc.withHeader("x", "y", "z")
    val s    = saver(path, cfg)
    s.run[IO].unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
    checkHeader(path, "x,y,z")
  }

  test("7.tablet read/write identity with-implicit-header") {
    val path = root / "header_implicit"
    val cfg  = CsvConfiguration.rfc.withHeader
    val s    = saver(path, cfg)
    s.run[IO].unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
    checkHeader(path, csvHeader(cfg).head.get.dropRight(2))
  }

  test("8.tablet read/write identity with-header-delimiter") {
    val path = root / "header_delimiter"
    val cfg  = CsvConfiguration.rfc.withCellSeparator('|').withHeader("a", "b")
    val s    = saver(path, cfg)
    s.run[IO].unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
    checkHeader(path, "a|b")
  }

  test("9.tablet read/write identity with-header-delimiter-quote") {
    val path = root / "header_delimiter_quote"
    val cfg  = CsvConfiguration.rfc.withHeader("", "b", "").withCellSeparator('|').withQuote('*').quoteAll
    val s    = saver(path, cfg)
    s.run[IO].unsafeRunSync()
    val t = loaders.rdd.kantan[Tablet](path, sparkSession, cfg)
    assert(data.toSet == t.collect().toSet)
    assert(data.toSet == loadTablet(path, cfg).unsafeRunSync())
    checkHeader(path, "**|*b*|**")
  }
}
