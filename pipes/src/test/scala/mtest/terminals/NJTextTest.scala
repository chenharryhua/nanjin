package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.{toFunctorFilterOps, toTraverseOps}
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.terminals.*
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.generic.auto.*
import io.circe.jawn
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.Tiger
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

class NJTextTest extends AnyFunSuite {
  val zoneId: ZoneId = ZoneId.systemDefault()

  def fs2(path: Url, file: TextFile, data: Set[Tiger]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val ts = Stream.emits(data.toList).covary[IO].map(_.asJson.noSpaces)
    val sink = hdp.sink(tgt).text
    val src: Stream[IO, Tiger] = hdp.source(tgt).text(2).mapFilter(decode[Tiger](_).toOption)
    val action: IO[List[Tiger]] = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
    val fileName = (file: FileKind).asJson.noSpaces
    assert(jawn.decode[FileKind](fileName).toOption.get == file)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()
    assert(size == data.size)
    assert(
      hdp
        .source(tgt)
        .text(100)
        .mapFilter(decode[Tiger](_).toOption)
        .compile
        .toList
        .unsafeRunSync()
        .toSet == data)
  }

  val fs2Root: Url = Url.parse("./data/test/terminals/text/tiger")

  test("uncompressed") {
    fs2(fs2Root, TextFile(_.Uncompressed), TestData.tigerSet)
  }

  test("gzip") {
    fs2(fs2Root, TextFile(_.Gzip), TestData.tigerSet)
  }

  test("snappy") {
    fs2(fs2Root, TextFile(_.Snappy), TestData.tigerSet)
  }

  test("bzip2") {
    fs2(fs2Root, TextFile(_.Bzip2), TestData.tigerSet)
  }

  test("lz4") {
    fs2(fs2Root, TextFile(_.Lz4), TestData.tigerSet)
  }

  test("deflate - 1") {
    fs2(fs2Root, TextFile(_.Deflate(8)), TestData.tigerSet)
  }

  test("laziness") {
    hdp.source("./does/not/exist").text(2)
    hdp.sink("./does/not/exist").text
  }

  test("rotation - policy") {
    val path = fs2Root / "rotation" / "tick"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val fk = TextFile(_.Uncompressed)
    val processedSize = Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.toString)
      .through(hdp.rotateSink(zoneId, Policy.fixedDelay(1.second))(t => path / fk.fileName(t)).text)
      .fold(0L)((sum, v) => sum + v.value.recordCount)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).text(2).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 10)
    assert(processedSize == number * 10)

  }

  test("rotation - size") {
    val path = fs2Root / "rotation" / "index"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val fk = TextFile(_.Uncompressed)
    val processedSize = Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.toString)
      .through(hdp.rotateSink(1000)(t => path / fk.fileName(t)).text)
      .fold(0L)((sum, v) => sum + v.value.recordCount)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).text(2).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 10)
    assert(processedSize == number * 10)

  }

  test("rotation - empty") {
    val path = fs2Root / "rotation" / "empty"
    hdp.delete(path).unsafeRunSync()
    val fk = TextFile(_.Uncompressed)
    (Stream.sleep[IO](10.hours) >>
      Stream.empty.covaryAll[IO, String])
      .through(
        hdp.rotateSink(zoneId, Policy.fixedDelay(1.second).limited(3))(t => path / fk.fileName(t)).text)
      .compile
      .drain
      .unsafeRunSync()
    import better.files.*
    hdp.filesIn(path).unsafeRunSync().foreach(np => assert(File(np.toJavaURI).lines.isEmpty))
  }

  test("stream concat") {
    val s = Stream.emits(TestData.tigerSet.toList).covary[IO].repeatN(500).map(_.toString)
    val path: Url = fs2Root / "concat" / "kantan.csv"

    (hdp.delete(path) >>
      (s ++ s ++ s).through(hdp.sink(path).text).compile.drain).unsafeRunSync()
    val size = hdp.source(path).text(100).compile.fold(0) { case (s, _) => s + 1 }.unsafeRunSync()
    assert(size == 15000)
  }

  ignore("large number (10000) of files - passed but too cost to run it") {
    val path = fs2Root / "rotation" / "many"
    val number = 1000L
    val file = TextFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.toString)
      .through(hdp.rotateSink(1)(t => path / file.fileName(t)).text)
      .fold(0L)((sum, v) => sum + v.value.recordCount)
      .compile
      .lastOrError
      .unsafeRunSync()
  }
}
