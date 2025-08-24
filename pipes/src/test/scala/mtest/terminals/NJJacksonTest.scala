package mtest.terminals
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.terminals.{FileKind, JacksonFile}
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.jawn
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import org.apache.avro.generic.GenericRecord
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.DurationDouble

class NJJacksonTest extends AnyFunSuite {
  import HadoopTestData.*

  def fs2(path: Url, file: JacksonFile, data: Set[GenericRecord]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val sink = hdp.sink(tgt).jackson
    val src = hdp.source(tgt).jackson(10, pandaSchema)
    val ts = Stream.emits(data.toList).covary[IO]
    val action = ts.through(sink).compile.drain >> src.compile.toList.map(_.toList)
    assert(action.unsafeRunSync().toSet == data)
    val fileName = (file: FileKind).asJson.noSpaces
    assert(jawn.decode[FileKind](fileName).toOption.get == file)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()
    assert(size == data.size)
    assert(hdp.source(tgt).jackson(10, pandaSchema).compile.toList.unsafeRunSync().toSet == data)

  }

  val fs2Root: Url = Url.parse("./data/test/terminals/jackson/panda")
  test("1.uncompressed") {
    fs2(fs2Root, JacksonFile(_.Uncompressed), pandaSet)
  }

  test("2.gzip") {
    fs2(fs2Root, JacksonFile(_.Gzip), pandaSet)
  }

  test("3.snappy") {
    fs2(fs2Root, JacksonFile(_.Snappy), pandaSet)
  }

  test("4.bzip2") {
    fs2(fs2Root, JacksonFile(_.Bzip2), pandaSet)
  }

  test("5.lz4") {
    fs2(fs2Root, JacksonFile(_.Lz4), pandaSet)
  }

  test("6.deflate - 1") {
    fs2(fs2Root, JacksonFile(_.Deflate(5)), pandaSet)
  }

  test("7.laziness") {
    hdp.source("./does/not/exist").jackson(10, pandaSchema)
    hdp.sink("./does/not/exist").jackson
  }

  test("8.rotation - policy") {
    val path = fs2Root / "rotation" / "tick"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val file = JacksonFile(_.Uncompressed)
    val processedSize = Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(hdp
        .rotateSink(Policy.fixedDelay(0.2.second), ZoneId.systemDefault())(t => path / file.fileName(t))
        .jackson)
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).jackson(10, pandaSchema).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(processedSize == number * 2)
  }

  test("9.rotation - size") {
    val path = fs2Root / "rotation" / "index"
    val number = 10000L
    val file = JacksonFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    val tickedValues = Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .chunkN(300)
      .unchunks
      .through(hdp.rotateSink(1000)(t => path / file.fileName(t)).jackson)
      .debug(_.asJson.noSpaces)
      .compile
      .toList
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).jackson(10, pandaSchema).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(tickedValues.map(_.value).sum == number * 2)

    assert(tickedValues.head.value == 1000)
    assert(tickedValues.head.tick.index == 1)
    assert(tickedValues(1).value == 1000)
    assert(tickedValues(1).tick.index == 2)
    assert(tickedValues(2).value == 1000)
    assert(tickedValues(2).tick.index == 3)
    assert(tickedValues(3).value == 1000)
    assert(tickedValues(3).tick.index == 4)

    assert(tickedValues(4).value == 1000)
    assert(tickedValues(4).tick.index == 5)
    assert(tickedValues(5).value == 1000)
    assert(tickedValues(5).tick.index == 6)
    assert(tickedValues(6).value == 1000)
    assert(tickedValues(6).tick.index == 7)
    assert(tickedValues(7).value == 1000)
    assert(tickedValues(7).tick.index == 8)

    assert(tickedValues(8).value == 1000)
    assert(tickedValues(8).tick.index == 9)
    assert(tickedValues(9).value == 1000)
    assert(tickedValues(9).tick.index == 10)

  }

  test("10.stream concat") {
    val s = Stream.emits(pandaSet.toList).covary[IO].repeatN(500)
    val path: Url = fs2Root / "concat" / "jackson.json"

    (hdp.delete(path) >>
      (s ++ s ++ s).through(hdp.sink(path).jackson).compile.drain).unsafeRunSync()
    val size =
      hdp.source(path).jackson(100, pandaSchema).compile.fold(0) { case (s, _) => s + 1 }.unsafeRunSync()
    assert(size == 3000)
  }

  test("11.stream concat - 2") {
    val s = Stream.emits(pandaSet.toList).covary[IO].repeatN(500)
    val path: Url = fs2Root / "concat" / "rotate"
    val sink = hdp.rotateSink(Policy.fixedDelay(0.1.second), ZoneId.systemDefault())(t =>
      path / JacksonFile(_.Uncompressed).fileName(t))

    (hdp.delete(path) >>
      (s ++ s ++ s).through(sink.jackson).compile.drain).unsafeRunSync()
  }

  test("12.timeout") {
    val path = fs2Root / "rotation" / "timeout"
    val number = 500000000L
    val file = JacksonFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    val res = Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(hdp
        .rotateSink(Policy.fixedDelay(3.seconds), ZoneId.systemDefault())(t => path / file.fileName(t))
        .jackson)
      .fold(0L)((sum, v) => sum + v.value)
      .timeout(4.seconds)
      .compile
      .lastOrError
    assertThrows[Throwable](res.unsafeRunSync())
    assert(better.files.File(path.toString()).list(_.extension.contains(".json")).size == 2)
  }

  ignore("large number (10000) of files - passed but too cost to run it") {
    val path = fs2Root / "rotation" / "many"
    val number = 5000L
    val file = JacksonFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(hdp.rotateSink(1)(t => path / file.fileName(t)).jackson)
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
  }
}
