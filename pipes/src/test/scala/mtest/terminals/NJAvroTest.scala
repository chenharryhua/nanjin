package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.datetime.policies
import com.github.chenharryhua.nanjin.terminals.*
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.avro.generic.GenericRecord
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class NJAvroTest extends AnyFunSuite {
  import HadoopTestData.*

  val avro: HadoopAvro[IO] = hdp.avro(pandaSchema)

  def fs2(path: NJPath, file: AvroFile, data: Set[GenericRecord]): Assertion = {
    val tgt = path / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val sink   = avro.withChunkSize(100).withBlockSizeHint(1000).withCompression(file.compression).sink(tgt)
    val src    = avro.source(tgt)
    val ts     = Stream.emits(data.toList).covary[IO]
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/avro/panda")

  test("snappy avro") {
    fs2(fs2Root, AvroFile(Snappy), pandaSet)
  }

  test("deflate 6 avro") {
    fs2(fs2Root, AvroFile(Deflate(6)), pandaSet)
  }

  test("uncompressed avro") {
    fs2(fs2Root, AvroFile(Uncompressed), pandaSet)
  }

  test("xz 1 avro") {
    fs2(fs2Root, AvroFile(Xz(1)), pandaSet)
  }

  test("bzip2 avro") {
    fs2(fs2Root, AvroFile(Bzip2), pandaSet)
  }

  test("zstandard avro") {
    fs2(fs2Root, AvroFile(Zstandard(1)), pandaSet)
  }

  test("laziness") {
    avro.source(NJPath("./does/not/exist"))
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
      .through(avro.sink(policies.constantDelay[IO](1.second))(t => path / file.fileName(t)))
      .compile
      .drain
      .unsafeRunSync()
    val size = Stream.force(hdp.filesIn(path).map(avro.source)).compile.toList.map(_.size).unsafeRunSync()
    assert(size == number * 2)
  }
}
