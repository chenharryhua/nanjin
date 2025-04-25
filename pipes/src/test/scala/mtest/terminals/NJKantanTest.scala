package mtest.terminals
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.terminals.{CsvHeaderOf, FileKind, KantanFile}
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.jawn
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import kantan.csv.{CsvConfiguration, RowDecoder, RowEncoder}
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.*
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

class NJKantanTest extends AnyFunSuite {
  val tigerEncoder: RowEncoder[Tiger]          = shapeless.cachedImplicit
  val tigerDecoder: RowDecoder[Tiger]          = shapeless.cachedImplicit
  implicit val tigerHeader: CsvHeaderOf[Tiger] = shapeless.cachedImplicit

  def fs2(path: Url, file: KantanFile, csvConfiguration: CsvConfiguration, data: Set[Tiger]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val ts     = Stream.emits(data.toList).covary[IO].map(tigerEncoder.encode)
    val sink   = hdp.sink(tgt).kantan(csvConfiguration)
    val src    = hdp.source(tgt).kantan(100, csvConfiguration).map(tigerDecoder.decode).rethrow
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
    val fileName = (file: FileKind).asJson.noSpaces
    assert(jawn.decode[FileKind](fileName).toOption.get == file)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()
    assert(size == data.size)
    assert(
      hdp
        .source(tgt)
        .kantan(100, csvConfiguration)
        .map(tigerDecoder.decode)
        .rethrow
        .compile
        .toList
        .unsafeRunSync()
        .toSet == data)
  }

  val fs2Root: Url = Url.parse("./data/test/terminals/csv/tiger")

  test("uncompressed - with-header") {
    val cfg = CsvConfiguration.rfc.withHeader(tigerHeader.modify(_ + "_tiger"))
    fs2(fs2Root / "header", KantanFile(_.Uncompressed), cfg, tigerSet)
  }

  test("uncompressed - with-implicit-header") {
    val cfg = CsvConfiguration.rfc.withHeader
    fs2(fs2Root / "header-implicit", KantanFile(_.Uncompressed), cfg, tigerSet)
  }

  test("uncompressed - without-header") {
    val cfg = CsvConfiguration.rfc.withHeader(false)
    fs2(fs2Root / "no-header", KantanFile(_.Uncompressed), cfg, tigerSet)
  }

  test("gzip") {
    val cfg = CsvConfiguration.rfc
    fs2(fs2Root, KantanFile(_.Gzip), cfg, tigerSet)
  }
  test("snappy") {
    val cfg = CsvConfiguration.rfc
    fs2(fs2Root, KantanFile(_.Snappy), cfg, tigerSet)
  }
  test("bzip2") {
    val cfg = CsvConfiguration.rfc.withCellSeparator('?')
    fs2(fs2Root, KantanFile(_.Bzip2), cfg, tigerSet)
  }
  test("lz4") {
    val cfg = CsvConfiguration.rfc.withQuotePolicy(CsvConfiguration.QuotePolicy.WhenNeeded)
    fs2(fs2Root, KantanFile(_.Lz4), cfg, tigerSet)
  }

  test("deflate") {
    val cfg = CsvConfiguration.rfc.withQuote('*')
    fs2(fs2Root, KantanFile(_.Deflate(6)), cfg, tigerSet)
  }

  test("laziness") {
    hdp.source("./does/not/exist").kantan(100, CsvConfiguration.rfc)
    hdp.sink("./does/not/exist").kantan(CsvConfiguration.rfc)
  }

  val policy: Policy = Policy.fixedDelay(1.second)
  test("rotation - with-header - tick") {
    val path = fs2Root / "rotation" / "header" / "tick"
    val file = KantanFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    herd
      .map(tigerEncoder.encode)
      .through(
        hdp
          .rotateSink(policy, ZoneId.systemDefault())(t => path / file.fileName(t))
          .kantan(_.withHeader(CsvHeaderOf[Tiger].header)))
      .compile
      .drain
      .unsafeRunSync()

    val size =
      hdp
        .filesIn(path)
        .flatMap(
          _.traverse(
            hdp
              .source(_)
              .kantan(1000, _.withHeader)
              .map(tigerDecoder.decode)
              .rethrow
              .compile
              .toList
              .map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == herd_number)
  }

  test("rotation - with-header - index") {
    val path = fs2Root / "rotation" / "header" / "index"
    hdp.delete(path).unsafeRunSync()
    val file = KantanFile(_.Uncompressed)
    herd
      .map(tigerEncoder.encode)
      .through(
        hdp.rotateSink(1000)(t => path / file.fileName(t)).kantan(_.withHeader(CsvHeaderOf[Tiger].header)))
      .compile
      .drain
      .unsafeRunSync()

    val size =
      hdp
        .filesIn(path)
        .flatMap(
          _.traverse(
            hdp
              .source(_)
              .kantan(1000, _.withHeader)
              .map(tigerDecoder.decode)
              .rethrow
              .compile
              .toList
              .map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == herd_number)
  }

  test("rotation - empty(with header)") {
    val path = fs2Root / "rotation" / "header" / "empty"
    hdp.delete(path).unsafeRunSync()
    val fk = KantanFile(_.Uncompressed)
    (Stream.sleep[IO](10.hours) >>
      Stream.empty.covaryAll[IO, Seq[String]])
      .through(
        hdp
          .rotateSink(Policy.fixedDelay(1.second).limited(3), ZoneId.systemDefault())(t =>
            path / fk.fileName(t))
          .kantan(_.withHeader(CsvHeaderOf[Tiger].header)))
      .compile
      .drain
      .unsafeRunSync()
    import better.files.*
    hdp.filesIn(path).unsafeRunSync().foreach(np => assert(File(np.toJavaURI).lines.size == 1))
  }

  test("rotation - no header - tick") {
    val path   = fs2Root / "rotation" / "no-header" / "tick"
    val number = 10000L
    val file   = KantanFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    herd
      .map(tigerEncoder.encode)
      .through(
        hdp.rotateSink(policy, ZoneId.systemDefault())(t => path / file.fileName(t)).kantan.andThen(_.drain))
      .map(tigerDecoder.decode)
      .rethrow
      .compile
      .drain
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(
          _.traverse(hdp.source(_).kantan(1000).map(tigerDecoder.decode).rethrow.compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number)
  }

  test("rotation - no header - index") {
    val path   = fs2Root / "rotation" / "no-header" / "index"
    val number = 10000L
    val file   = KantanFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    herd
      .map(tigerEncoder.encode)
      .through(hdp.rotateSink(1000)(t => path / file.fileName(t)).kantan.andThen(_.drain))
      .map(tigerDecoder.decode)
      .rethrow
      .compile
      .drain
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(
          _.traverse(hdp.source(_).kantan(1000).map(tigerDecoder.decode).rethrow.compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number)
  }

  test("rotation - empty(no header)") {
    val path = fs2Root / "rotation" / "no-header" / "empty"
    hdp.delete(path).unsafeRunSync()
    val fk = KantanFile(_.Uncompressed)
    (Stream.sleep[IO](10.hours) >>
      Stream.empty.covaryAll[IO, Seq[String]])
      .through(
        hdp
          .rotateSink(Policy.fixedDelay(1.second).limited(3), ZoneId.systemDefault())(t =>
            path / fk.fileName(t))
          .kantan)
      .compile
      .drain
      .unsafeRunSync()
    import better.files.*
    hdp.filesIn(path).unsafeRunSync().foreach(np => assert(File(np.toJavaURI).lines.isEmpty))
  }

  test("stream concat") {
    val s         = Stream.emits(TestData.tigerSet.toList).covary[IO].repeatN(500).map(tigerEncoder.encode)
    val path: Url = fs2Root / "concat" / "kantan.csv"

    (hdp.delete(path) >>
      (s ++ s ++ s).through(hdp.sink(path).kantan).compile.drain).unsafeRunSync()
    val size = hdp.source(path).kantan(100).compile.fold(0) { case (s, _) => s + 1 }.unsafeRunSync()
    assert(size == 15000)
  }

  ignore("large number (10000) of files - passed but too cost to run it") {
    val path   = fs2Root / "rotation" / "many"
    val number = 1000L
    val file   = KantanFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(tigerEncoder.encode)
      .through(hdp.rotateSink(1)(t => path / file.fileName(t)).kantan)
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
  }
}
