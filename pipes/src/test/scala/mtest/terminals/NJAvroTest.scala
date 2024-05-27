package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.terminals.*
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.syntax.EncoderOps
import org.apache.avro.generic.GenericRecord
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.DurationInt
import io.circe.jawn
class NJAvroTest extends AnyFunSuite {
  import HadoopTestData.*

  val avro: HadoopAvro[IO] = hdp.avro(pandaSchema)

  def fs2(path: NJPath, file: AvroFile, data: Set[GenericRecord]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val sink     = avro.withCompression(file.compression).sink(tgt)
    val src      = avro.source(tgt, 100)
    val ts       = Stream.emits(data.toList).covary[IO]
    val action   = ts.through(sink).compile.drain >> src.compile.toList
    val fileName = (file: NJFileKind).asJson.noSpaces
    assert(jawn.decode[NJFileKind](fileName).toOption.get == file)
    assert(action.unsafeRunSync().toSet == data)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/avro/panda")

  test("snappy avro") {
    fs2(fs2Root, AvroFile(_.Snappy), pandaSet)
  }

  test("deflate 6 avro") {
    fs2(fs2Root, AvroFile(_.Deflate(6)), pandaSet)
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
    avro.source(NJPath("./does/not/exist"), 100)
    avro.sink(NJPath("./does/not/exist"))
  }

  test("rotation") {
    val path   = fs2Root / "rotation"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val file = AvroFile(Uncompressed)
    Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(avro.sink(policies.fixedDelay(1.second), ZoneId.systemDefault())(t => path / file.fileName(t)))
      .compile
      .drain
      .unsafeRunSync()
    val size =
      Stream.eval(hdp.filesIn(path)).flatMap(avro.source(_, 100)).compile.toList.map(_.size).unsafeRunSync()
    assert(size == number * 2)
  }
}
