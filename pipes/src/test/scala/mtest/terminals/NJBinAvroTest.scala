package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.terminals.{BinAvroFile, FileKind}
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

class NJBinAvroTest extends AnyFunSuite {
  import HadoopTestData.*

  def fs2(path: Url, file: BinAvroFile, data: Set[GenericRecord]): Assertion = {
    val tgt = path / file.fileName

    hdp.delete(tgt).unsafeRunSync()
    val sink = hdp.sink(tgt).binAvro
    val src = hdp.source(tgt).binAvro(100, pandaSchema)
    val ts = Stream.emits(data.toList).covary[IO]
    val action = ts.through(sink).compile.drain >> src.compile.toList
    val fileName = (file: FileKind).asJson.noSpaces
    assert(jawn.decode[FileKind](fileName).toOption.get == file)
    assert(action.unsafeRunSync().toSet == data)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()

    hdp.source(tgt).binAvro(100, pandaSchema, readerSchema).debug().compile.drain.unsafeRunSync()

    assert(size == data.size)
    assert(hdp.source(tgt).binAvro(100, pandaSchema).compile.toList.unsafeRunSync().toSet == data)
  }

  val fs2Root: Url = "data/test/terminals/bin_avro/panda"

  test("uncompressed") {
    fs2(fs2Root, BinAvroFile(_.Uncompressed), pandaSet)
  }

  test("gzip") {
    fs2(fs2Root, BinAvroFile(_.Gzip), pandaSet)
  }

  test("snappy") {
    fs2(fs2Root, BinAvroFile(_.Snappy), pandaSet)
  }

  test("bzip2") {
    fs2(fs2Root, BinAvroFile(_.Bzip2), pandaSet)
  }

  test("lz4") {
    fs2(fs2Root, BinAvroFile(_.Lz4), pandaSet)
  }

  test("deflate - 1") {
    fs2(fs2Root, BinAvroFile(_.Deflate(2)), pandaSet)
  }

  test("laziness") {
    hdp.source("./does/not/exist").binAvro(10, pandaSchema)
    hdp.sink("./does/not/exist").binAvro
  }

  test("rotation - policy") {
    val path = fs2Root / "rotation" / "tick"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val file = BinAvroFile(_.Uncompressed)
    val processedSize = Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(
        hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(1.second))(t => path / file.fileName(t)).binAvro)
      .fold(0L)((sum, v) => sum + v.value.recordCount)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).binAvro(10, pandaSchema).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(processedSize == number * 2)
  }

  test("rotation - size") {
    val path = fs2Root / "rotation" / "index"
    val number = 10000L
    val file = BinAvroFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    val processedSize = Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .chunkN(1000)
      .unchunks
      .through(hdp.rotateSink(sydneyTime, 1000)(t => path / file.fileName(t)).binAvro)
      .fold(0L)((sum, v) => sum + v.value.recordCount)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(hdp.source(_).binAvro(10, pandaSchema).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(processedSize == number * 2)
  }

  test("stream concat") {
    val s = Stream.emits(pandaSet.toList).covary[IO].repeatN(500)
    val path: Url = fs2Root / "concat" / "bin.avro"

    (hdp.delete(path) >>
      (s ++ s ++ s).through(hdp.sink(path).binAvro).compile.drain).unsafeRunSync()
    val size =
      hdp.source(path).binAvro(100, pandaSchema).compile.fold(0) { case (s, _) => s + 1 }.unsafeRunSync()
    assert(size == 3000)
  }

  test("stream concat - 2") {
    val s = Stream.emits(pandaSet.toList).covary[IO].repeatN(500)
    val path: Url = fs2Root / "concat" / "rotate"
    val sink = hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(0.1.second))(t =>
      path / BinAvroFile(_.Uncompressed).fileName(t))

    (hdp.delete(path) >>
      (s ++ s ++ s).through(sink.binAvro).compile.drain).unsafeRunSync()
  }

  ignore("large number (10000) of files - passed but too cost to run it") {
    val path = fs2Root / "rotation" / "many"
    val number = 5000L
    val file = BinAvroFile(_.Uncompressed)
    hdp.delete(path).unsafeRunSync()
    Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(hdp.rotateSink(sydneyTime, 1000)(t => path / file.fileName(t)).binAvro)
      .fold(0L)((sum, v) => sum + v.value.recordCount)
      .compile
      .lastOrError
      .unsafeRunSync()
  }
}
