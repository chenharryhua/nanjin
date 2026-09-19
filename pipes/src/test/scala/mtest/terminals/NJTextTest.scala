package mtest.terminals

import cats.effect.IO
import cats.implicits.{toFunctorFilterOps, toTraverseOps}
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.terminals.*
import fs2.Stream
import io.circe.generic.auto.*
import io.circe.jawn
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import munit.CatsEffectSuite
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.Tiger

import java.time.ZoneId
import scala.concurrent.duration.*

class NJTextTest extends CatsEffectSuite {
  val zoneId: ZoneId = ZoneId.systemDefault()

  def fs2(path: Url, file: TextFile, data: Set[Tiger]): IO[Unit] = {
    val tgt = path / file.fileName
    val ts = Stream.emits(data.toList).covary[IO].map(_.asJson.noSpaces)
    val sink = hdp.sink(tgt).text
    val src: Stream[IO, Tiger] = hdp.source(tgt).text(2).mapFilter(decode[Tiger](_).toOption)
    val action: IO[List[Tiger]] = ts.through(sink).compile.drain >> src.compile.toList
    val fileName = (file: FileKind).asJson.noSpaces
    for {
      _ <- hdp.delete(tgt)
      actionResult <- action
      _ = assert(actionResult.toSet == data)
      _ = assert(jawn.decode[FileKind](fileName).toOption.get == file)
      size <- ts.through(sink).fold(0)(_ + _).compile.lastOrError
      _ = assert(size == data.size)
      roundTrip <-
        hdp
          .source(tgt)
          .text(100)
          .mapFilter(decode[Tiger](_).toOption)
          .compile
          .toList
    } yield assert(roundTrip.toSet == data)
  }

  val fs2Root: Url = Url.parse("./data/test/terminals/text/tiger")

  test("1.uncompressed") {
    fs2(fs2Root, TextFile(_.Uncompressed), TestData.tigerSet)
  }

  test("2.gzip") {
    fs2(fs2Root, TextFile(_.Gzip), TestData.tigerSet)
  }

  test("3.snappy") {
    fs2(fs2Root, TextFile(_.Snappy), TestData.tigerSet)
  }

  test("4.bzip2") {
    fs2(fs2Root, TextFile(_.Bzip2), TestData.tigerSet)
  }

  test("5.lz4") {
    fs2(fs2Root, TextFile(_.Lz4), TestData.tigerSet)
  }

  test("6.deflate - 1") {
    fs2(fs2Root, TextFile(_.Deflate(_.Eight)), TestData.tigerSet)
  }

  test("7.laziness") {
    hdp.source("./does/not/exist").text(2)
    hdp.sink("./does/not/exist").text
  }

  test("8.rotation - policy") {
    val path = fs2Root / "rotation" / "tick"
    val number = 10000L
    val fk = TextFile(_.Uncompressed)
    for {
      _ <- hdp.delete(path)
      processedSize <- Stream
        .emits(TestData.tigerSet.toList)
        .covary[IO]
        .repeatN(number)
        .map(_.toString)
        .through(hdp.rotateSink(zoneId, _.fixedDelay(100.milliseconds).repeat)(t =>
          path / fk.fileName(t)).text)
        .evalTap(tv => IO.println(tv.window))
        .debug(_.asJson.noSpaces)
        .fold(0L)((sum, v) => sum + v.recordCount)
        .compile
        .lastOrError
      size <-
        hdp
          .filesIn(path)
          .flatMap(_.traverse(hdp.source(_).text(2).compile.toList.map(_.size)))
          .map(_.sum)
    } yield {
      assert(size == number * 10)
      assert(processedSize == number * 10)
    }
  }

  test("9.rotation - size") {
    val path = fs2Root / "rotation" / "index"
    val number = 10002L
    val fk = TextFile(_.Uncompressed)
    for {
      _ <- hdp.delete(path)
      processedSize <- Stream
        .emits(TestData.tigerSet.toList)
        .covary[IO]
        .repeatN(number)
        .map(_.toString)
        .through(hdp.rotateSink(sydneyTime, 15000)(t => path / fk.fileName(t)).text)
        .debug(_.asJson.noSpaces)
        .fold(0L)((sum, v) => sum + v.recordCount)
        .compile
        .lastOrError
      size <-
        hdp
          .filesIn(path)
          .flatMap(_.traverse(hdp.source(_).text(2).compile.toList.map(_.size)))
          .map(_.sum)
    } yield {
      assert(size == number * 10)
      assert(processedSize == number * 10)
    }
  }

  test("10.rotation - empty") {
    val path = fs2Root / "rotation" / "empty"
    val fk = TextFile(_.Uncompressed)
    import better.files.*
    for {
      _ <- hdp.delete(path)
      _ <- (Stream.sleep[IO](10.hours) >>
        Stream.empty.covaryAll[IO, String])
        .through(hdp.rotateSink(zoneId, _.fixedDelay(1.second).repeat.limited(3))(t =>
          path / fk.fileName(t)).text)
        .compile
        .drain
      files <- hdp.filesIn(path)
    } yield files.foreach(np => assert(File(np.toJavaURI).lines.isEmpty))
  }

  test("11.stream concat") {
    val s = Stream.emits(TestData.tigerSet.toList).covary[IO].repeatN(500).map(_.toString)
    val path: Url = fs2Root / "concat" / "kantan.csv"

    for {
      _ <- hdp.delete(path) >>
        (s ++ s ++ s).through(hdp.sink(path).text).compile.drain
      size <- hdp.source(path).text(100).compile.fold(0) { case (s, _) => s + 1 }
    } yield assert(size == 15000)
  }

  test("large number (10000) of files - passed but too cost to run it".ignore) {
    val path = fs2Root / "rotation" / "many"
    val number = 1000L
    val file = TextFile(_.Uncompressed)
    hdp.delete(path) >> Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.toString)
      .through(hdp.rotateSink(sydneyTime, 1)(t => path / file.fileName(t)).text)
      .fold(0L)((sum, v) => sum + v.recordCount)
      .compile
      .lastOrError
      .void
  }
}
