package mtest.terminals

import cats.effect.IO
import cats.implicits.{toFunctorFilterOps, toTraverseOps}
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.terminals.*
import com.github.chenharryhua.nanjin.terminals.Compression.*
import fs2.text.{lines, utf8}
import fs2.{Pipe, Stream}
import io.circe.generic.auto.*
import io.circe.jawn.CirceSupportParser.facade
import io.circe.syntax.EncoderOps
import io.circe.{jawn, Json}
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import munit.CatsEffectSuite
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.Tiger
import org.typelevel.jawn.fs2.JsonStreamSyntax
import squants.information.InformationConversions.InformationConversions
import java.time.ZoneId
import scala.concurrent.duration.{DurationDouble, DurationInt}

class NJCirceTest extends CatsEffectSuite {

  def fs2(path: Url, file: CirceFile, data: Set[Tiger]): IO[Unit] = {
    val tgt = path / file.fileName
    val ts: Stream[IO, Json] = Stream.emits(data.toList).covary[IO].map(_.asJson)
    val sink: Pipe[IO, Json, Int] = hdp.sink(tgt).circe
    val src: Stream[IO, Tiger] = hdp.source(tgt).circe(10).mapFilter(_.as[Tiger].toOption)
    val action: IO[List[Tiger]] = ts.through(sink).compile.drain >> src.compile.toList
    val lineCount = hdp.source(tgt).text(32).compile.fold(0) { case (s, _) => s + 1 }
    val fileName = (file: FileKind).asJson.noSpaces
    for {
      _ <- hdp.delete(tgt)
      actionResult <- action
      _ = assert(actionResult.toSet == data)
      lineResult <- lineCount
      _ = assert(lineResult == data.size)
      _ = assert(jawn.decode[FileKind](fileName).toOption.get == file)
      size <- ts.through(sink).fold(0)(_ + _).compile.lastOrError
      _ = assert(size == data.size)
      roundTrip <- hdp.source(tgt).circe(10).mapFilter(_.as[Tiger].toOption).compile.toList
    } yield assert(roundTrip.toSet == data)
  }

  val fs2Root: Url = Url.parse("./data/test/terminals/circe/tiger")

  test("1.uncompressed") {
    fs2(fs2Root, CirceFile(_.Uncompressed), TestData.tigerSet)
  }

  test("2.gzip") {
    fs2(fs2Root, CirceFile(_.Gzip), TestData.tigerSet)
  }

  test("3.snappy") {
    fs2(fs2Root, CirceFile(_.Snappy), TestData.tigerSet)
  }

  test("4.bzip2") {
    fs2(fs2Root, CirceFile(_.Bzip2), TestData.tigerSet)
  }

  test("5.lz4") {
    fs2(fs2Root, CirceFile(_.Lz4), TestData.tigerSet)
  }

  test("6.deflate - 1") {
    fs2(fs2Root, CirceFile(_.Deflate(_.Four)), TestData.tigerSet)
  }

  test("7.laziness") {
    hdp.source("./does/not/exist").circe(10)
    hdp.sink("./does/not/exist").circe
  }

  test("8.rotation - policy") {
    val path = fs2Root / "rotation" / "tick"
    val number = 10000L
    val fk = CirceFile(Uncompressed)

    def tigers1(path: Url): Stream[IO, Tiger] =
      hdp
        .source(path)
        .bytes(1.kb)
        .through(utf8.decode)
        .through(lines)
        .takeWhile(_.nonEmpty)
        .map(jawn.decode[Tiger])
        .rethrow

    def tigers2(path: Url): Stream[IO, Tiger] =
      hdp.source(path).bytes(1.kb).chunks.parseJsonStream.map(_.as[Tiger]).rethrow

    for {
      _ <- hdp.delete(path)
      processedSize <- Stream
        .emits(TestData.tigerSet.toList)
        .covary[IO]
        .repeatN(number)
        .map(_.asJson)
        .through(hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(0.1.second).repeat)(t =>
          path / fk.fileName(t)).circe)
        .fold(0L)((sum, v) => sum + v.recordCount)
        .compile
        .lastOrError
      size <-
        hdp
          .filesIn(path)
          .flatMap(_.traverse(hdp.source(_).circe(10).compile.toList.map(_.size)))
          .map(_.sum)
      _ = assert(size == number * TestData.tigerSet.toList.size)
      _ = assert(processedSize == number * TestData.tigerSet.toList.size)
      _ <- hdp
        .filesIn(path)
        .flatMap(_.traverse(p =>
          tigers1(p).interleave(tigers2(p)).chunkN(2).map(c => assert(c(0) == c(1))).compile.drain))
    } yield ()
  }

  test("9.rotation - size") {
    val path = fs2Root / "rotation" / "index"
    val number = 10000L
    val file = CirceFile(Uncompressed)

    def tigers1(path: Url): Stream[IO, Tiger] =
      hdp
        .source(path)
        .bytes(1.kb)
        .through(utf8.decode)
        .through(lines)
        .takeWhile(_.nonEmpty)
        .map(jawn.decode[Tiger])
        .rethrow

    def tigers2(path: Url): Stream[IO, Tiger] =
      hdp.source(path).bytes(1.kb).chunks.parseJsonStream.map(_.as[Tiger]).rethrow

    for {
      _ <- hdp.delete(path)
      processedSize <- Stream
        .emits(TestData.tigerSet.toList)
        .covary[IO]
        .repeatN(number)
        .map(_.asJson)
        .through(hdp.rotateSink(sydneyTime, 1000)(t => path / file.fileName(t)).circe)
        .map(_.asJson.noSpaces)
        .map(io.circe.jawn.decode[RotateFile])
        .rethrow
        .fold(0L)((sum, v) => sum + v.recordCount)
        .compile
        .lastOrError
      size <-
        hdp
          .filesIn(path)
          .flatMap(_.traverse(hdp.source(_).circe(10).compile.toList.map(_.size)))
          .map(_.sum)
      _ = assert(size == number * TestData.tigerSet.toList.size)
      _ = assert(processedSize == number * TestData.tigerSet.toList.size)
      _ <- hdp
        .filesIn(path)
        .flatMap(_.traverse(p =>
          tigers1(p).interleave(tigers2(p)).chunkN(2).map(c => assert(c(0) == c(1))).compile.drain))
    } yield ()
  }

  test("10.rotation - empty") {
    val path = fs2Root / "rotation" / "empty"
    val fk = CirceFile(Uncompressed)
    import better.files.*
    for {
      _ <- hdp.delete(path)
      _ <- (Stream.sleep[IO](10.hours) >>
        Stream.empty.covaryAll[IO, Json])
        .through(
          hdp
            .rotateSink(ZoneId.systemDefault(), _.fixedDelay(1.second).repeat.limited(3))(t =>
              path / fk.fileName(t))
            .circe)
        .compile
        .drain
      files <- hdp.filesIn(path)
    } yield files.foreach(np => assert(File(np.toJavaURI).lines.isEmpty))
  }

  test("11.stream concat") {
    val s = Stream.emits(TestData.tigerSet.toList).covary[IO].repeatN(500).map(_.asJson)
    val path: Url = fs2Root / "concat" / "circe.json"

    for {
      _ <- hdp.delete(path) >>
        (s ++ s ++ s).through(hdp.sink(path).circe).compile.drain
      size <- hdp.source(path).circe(10).compile.fold(0) { case (s, _) => s + 1 }
    } yield assert(size == 15000)
  }

  test("12.stream concat - 2") {
    val s = Stream.emits(TestData.tigerSet.toList).covary[IO].map(_.asJson).repeatN(500)
    val path: Url = fs2Root / "concat" / "rotate"
    val sink = hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(0.1.second).repeat)(t =>
      path / CirceFile(_.Uncompressed).fileName(t))

    hdp.delete(path) >>
      (s ++ s ++ s).through(sink.circe).compile.drain
  }

  test("13.emit in each time frame even if no data") {
    val path: Url = fs2Root / "empty"
    val sink =
      hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(1.second).repeat)(t => path / t.index.toString)
    val run = hdp.delete(path) >>
      Stream.sleep[IO](5.seconds).map(_ => Json.Null).through(sink.circe).compile.toList
    run.map(xs => assert(xs.size > 3))
  }

  test("large number (10000) of files - passed but too cost to run it".ignore) {
    val path = fs2Root / "rotation" / "many"
    val number = 1000L
    val file = CirceFile(_.Uncompressed)
    hdp.delete(path) >> Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.asJson)
      .through(hdp.rotateSink(sydneyTime, 1)(t => path / file.fileName(t)).circe)
      .fold(0L)((sum, v) => sum + v.recordCount)
      .compile
      .lastOrError
      .void
  }
}
