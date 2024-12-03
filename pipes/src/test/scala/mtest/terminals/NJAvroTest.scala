package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.terminals.*
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
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
import scala.concurrent.duration.DurationInt

class NJAvroTest extends AnyFunSuite {
  import HadoopTestData.*

  val avro: HadoopAvro[IO] = hdp.avro(pandaSchema)

  def fs2(path: Url, file: AvroFile, data: Set[GenericRecord]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val sink     = avro.withCompression(file.compression).sink(tgt)
    val src      = avro.source(tgt, 100)
    val ts       = Stream.emits(data.toList).covary[IO].chunks
    val action   = ts.through(sink).compile.drain >> src.compile.toList.map(_.toList)
    val fileName = (file: NJFileKind).asJson.noSpaces
    assert(jawn.decode[NJFileKind](fileName).toOption.get == file)
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
    avro.source("./does/not/exist", 100)
    avro.sink("./does/not/exist")
  }

  test("rotation") {
    val path   = fs2Root / "rotation"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val file = AvroFile(Uncompressed)
    val processedSize = Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .chunks
      .through(avro.rotateSink(Policy.fixedDelay(1.second), ZoneId.systemDefault())(t =>
        path / file.fileName(t)))
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .filesIn(path)
        .flatMap(_.traverse(avro.source(_, 100).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(processedSize == number * 2)
  }
}
