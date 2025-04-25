package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.terminals.*
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
import scala.concurrent.duration.{DurationDouble, DurationInt}

class NJAvroTest extends AnyFunSuite {
  import HadoopTestData.*

  def fs2(path: Url, file: AvroFile, data: Set[GenericRecord]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val sink     = hdp.sink(tgt).avro(file.compression)
    val src      = hdp.source(tgt).avro(100)
    val ts       = Stream.emits(data.toList).covary[IO]
    val action   = ts.through(sink).compile.drain >> src.compile.toList.map(_.toList)
    val fileName = (file: FileKind).asJson.noSpaces
    assert(jawn.decode[FileKind](fileName).toOption.get == file)
    assert(action.unsafeRunSync().toSet == data)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()
    assert(size == data.size)
    assert(hdp.source(tgt).avro(100).compile.toList.unsafeRunSync().toSet == data)
  }

  val fs2Root: Url = Url.parse("./data/test/terminals/avro/panda")

  test("snappy avro") {
    fs2(fs2Root, AvroFile(_.Snappy), pandaSet)
  }

  test("deflate 6 avro") {
    fs2("data/test/terminals/avro/panda", AvroFile(_.Deflate(6)), pandaSet)
  }

  test("uncompressed avro") {
    fs2(fs2Root, AvroFile(_.Uncompressed), pandaSet)
  }

  test("xz 1 avro") {
    fs2(fs2Root, AvroFile(_.Xz(1)), pandaSet)
  }

  test("bzip2 avro") {
    fs2(fs2Root, AvroFile(_.Bzip2), pandaSet)
  }

  test("zstandard avro") {
    fs2(fs2Root, AvroFile(_.Zstandard(1)), pandaSet)
  }

  test("laziness") {
    hdp.source("./does/not/exist").avro(100)
    hdp.sink("./does/not/exist").avro(_.Uncompressed)
  }

  test("rotation - tick") {
    val path   = fs2Root / "rotation" / "tick"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val file = AvroFile(_.Uncompressed)
    val processedSize = Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(hdp
        .rotateSink(Policy.fixedDelay(1.second), ZoneId.systemDefault())(t => path / file.fileName(t))
        .avro(_.Uncompressed))
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).avro(100).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(processedSize == number * 2)
  }

  test("rotation - index") {
    val path   = fs2Root / "rotation" / "index"
    val number = 10000L
    val file   = AvroFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    val processedSize = Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(hdp.rotateSink(1000)(t => path / file.fileName(t)).avro(_.Uncompressed))
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).avro(100).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(processedSize == number * 2)
  }

  test("stream concat") {
    val s         = Stream.emits(pandaSet.toList).covary[IO].repeatN(500)
    val path: Url = fs2Root / "concat" / "data.avro"

    (hdp.delete(path) >>
      (s ++ s ++ s).through(hdp.sink(path).avro).compile.drain).unsafeRunSync()
    val size = hdp.source(path).avro(100).compile.fold(0) { case (s, _) => s + 1 }.unsafeRunSync()
    assert(size == 3000)
  }

  test("stream concat - 2") {
    val s         = Stream.emits(pandaSet.toList).covary[IO].repeatN(500)
    val path: Url = fs2Root / "concat" / "rotate"
    val sink = hdp.rotateSink(Policy.fixedDelay(0.1.second), ZoneId.systemDefault())(t =>
      path / AvroFile(_.Uncompressed).fileName(t))

    (hdp.delete(path) >>
      (s ++ s ++ s).through(sink.avro).compile.drain).unsafeRunSync()
  }

  ignore("large number (10000) of files - passed but too cost to run it") {
    val path   = fs2Root / "rotation" / "many"
    val number = 5000L
    val file   = AvroFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(hdp.rotateSink(1000)(t => path / file.fileName(t)).avro(_.Uncompressed))
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
  }
}
