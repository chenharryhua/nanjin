package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.{toFunctorFilterOps, toTraverseOps}
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.terminals.*
import com.github.chenharryhua.nanjin.terminals.Compression.*
import eu.timepit.refined.auto.*
import fs2.text.{lines, utf8}
import fs2.{Pipe, Stream}
import io.circe.generic.auto.*
import io.circe.jawn.CirceSupportParser.facade
import io.circe.syntax.EncoderOps
import io.circe.{jawn, Json}
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.Tiger
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.jawn.fs2.JsonStreamSyntax
import squants.information.InformationConversions.InformationConversions

import java.time.ZoneId
import scala.concurrent.duration.{DurationDouble, DurationInt}

class NJCirceTest extends AnyFunSuite {

  def fs2(path: Url, file: CirceFile, data: Set[Tiger]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val ts: Stream[IO, Json] = Stream.emits(data.toList).covary[IO].map(_.asJson)
    val sink: Pipe[IO, Json, Int] = hdp.sink(tgt).circe
    val src: Stream[IO, Tiger] = hdp.source(tgt).circe(10).mapFilter(_.as[Tiger].toOption)
    val action: IO[List[Tiger]] = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
    val lines = hdp.source(tgt).text(32).compile.fold(0) { case (s, _) => s + 1 }
    assert(lines.unsafeRunSync() === data.size)
    val fileName = (file: FileKind).asJson.noSpaces
    assert(jawn.decode[FileKind](fileName).toOption.get == file)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()
    assert(size == data.size)
    assert(
      hdp.source(tgt).circe(10).mapFilter(_.as[Tiger].toOption).compile.toList.unsafeRunSync().toSet == data)
  }

  val fs2Root: Url = Url.parse("./data/test/terminals/circe/tiger")

  test("uncompressed") {
    fs2(fs2Root, CirceFile(_.Uncompressed), TestData.tigerSet)
  }

  test("gzip") {
    fs2(fs2Root, CirceFile(_.Gzip), TestData.tigerSet)
  }

  test("snappy") {
    fs2(fs2Root, CirceFile(_.Snappy), TestData.tigerSet)
  }

  test("bzip2") {
    fs2(fs2Root, CirceFile(_.Bzip2), TestData.tigerSet)
  }

  test("lz4") {
    fs2(fs2Root, CirceFile(_.Lz4), TestData.tigerSet)
  }

  test("deflate - 1") {
    fs2(fs2Root, CirceFile(_.Deflate(4)), TestData.tigerSet)
  }

  test("laziness") {
    hdp.source("./does/not/exist").circe(10)
    hdp.sink("./does/not/exist").circe
  }

  test("rotation - policy") {
    val path = fs2Root / "rotation" / "tick"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val fk = CirceFile(Uncompressed)
    val processedSize = Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.asJson)
      .through(
        hdp.rotateSink(Policy.fixedDelay(1.second), ZoneId.systemDefault())(t => path / fk.fileName(t)).circe)
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).circe(10).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * TestData.tigerSet.toList.size)
    assert(processedSize == number * TestData.tigerSet.toList.size)

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

    hdp
      .filesIn(path)
      .flatMap(_.traverse(p =>
        tigers1(p).interleave(tigers2(p)).chunkN(2).map(c => assert(c(0) == c(1))).compile.drain))
      .unsafeRunSync()
  }

  test("rotation - size") {
    val path = fs2Root / "rotation" / "index"
    val number = 10000L
    val file = CirceFile(Uncompressed)
    hdp.delete(path).unsafeRunSync()
    val processedSize = Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.asJson)
      .through(hdp.rotateSink(1000)(t => path / file.fileName(t)).circe)
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).circe(10).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * TestData.tigerSet.toList.size)
    assert(processedSize == number * TestData.tigerSet.toList.size)

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

    hdp
      .filesIn(path)
      .flatMap(_.traverse(p =>
        tigers1(p).interleave(tigers2(p)).chunkN(2).map(c => assert(c(0) == c(1))).compile.drain))
      .unsafeRunSync()
  }

  test("rotation - empty") {
    val path = fs2Root / "rotation" / "empty"
    hdp.delete(path).unsafeRunSync()
    val fk = CirceFile(Uncompressed)
    (Stream.sleep[IO](10.hours) >>
      Stream.empty.covaryAll[IO, Json])
      .through(
        hdp
          .rotateSink(Policy.fixedDelay(1.second).limited(3), ZoneId.systemDefault())(t =>
            path / fk.fileName(t))
          .circe)
      .compile
      .drain
      .unsafeRunSync()
    import better.files.*
    hdp.filesIn(path).unsafeRunSync().foreach(np => assert(File(np.toJavaURI).lines.isEmpty))
  }

  test("stream concat") {
    val s = Stream.emits(TestData.tigerSet.toList).covary[IO].repeatN(500).map(_.asJson)
    val path: Url = fs2Root / "concat" / "circe.json"

    (hdp.delete(path) >>
      (s ++ s ++ s).through(hdp.sink(path).circe).compile.drain).unsafeRunSync()
    val size = hdp.source(path).circe(10).compile.fold(0) { case (s, _) => s + 1 }.unsafeRunSync()
    assert(size == 15000)
  }

  test("stream concat - 2") {
    val s = Stream.emits(TestData.tigerSet.toList).covary[IO].map(_.asJson).repeatN(500)
    val path: Url = fs2Root / "concat" / "rotate"
    val sink = hdp.rotateSink(Policy.fixedDelay(0.1.second), ZoneId.systemDefault())(t =>
      path / ParquetFile(_.Uncompressed).fileName(t))

    (hdp.delete(path) >>
      (s ++ s ++ s).through(sink.circe).compile.drain).unsafeRunSync()
  }

  ignore("large number (10000) of files - passed but too cost to run it") {
    val path = fs2Root / "rotation" / "many"
    val number = 1000L
    val file = CirceFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.asJson)
      .through(hdp.rotateSink(1)(t => path / file.fileName(t)).circe)
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
  }
}
