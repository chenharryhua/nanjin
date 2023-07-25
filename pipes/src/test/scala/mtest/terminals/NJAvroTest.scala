package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.time.policies
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import com.github.chenharryhua.nanjin.terminals.*
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class NJAvroTest extends AnyFunSuite {
  import HadoopTestData.*

  val avro: HadoopAvro[IO] = hdp.avro(pandaSchema)

  def fs2(path: NJPath, codecFactory: CodecFactory, data: Set[GenericRecord]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val sink   = avro.withChunkSize(100).withBlockSizeHint(1000).withCodecFactory(codecFactory).sink(path)
    val src    = avro.source(path)
    val ts     = Stream.emits(data.toList).covary[IO]
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/avro/panda")

  test("snappy avro") {
    val file = AvroFile(Snappy)
    fs2(fs2Root / file.fileName, CodecFactory.snappyCodec, pandaSet)
  }

  test("deflate 6 avro") {
    val file = AvroFile(Deflate(6))
    fs2(fs2Root / file.fileName, CodecFactory.deflateCodec(6), pandaSet)
  }

  test("uncompressed avro") {
    val file = AvroFile(Uncompressed)
    fs2(fs2Root / file.fileName, CodecFactory.nullCodec(), pandaSet)
  }

  test("xz 1 avro") {
    val file = AvroFile(Xz(1))
    fs2(fs2Root / file.fileName, CodecFactory.xzCodec(1), pandaSet)
  }

  test("bzip2 avro") {
    val file = AvroFile(Bzip2)
    fs2(fs2Root / file.fileName, CodecFactory.bzip2Codec(), pandaSet)
  }

  test("zstandard avro") {
    val file = AvroFile(Zstandard(1))
    fs2(fs2Root / file.fileName, CodecFactory.zstandardCodec(1), pandaSet)
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
      .through(avro.sink(policies.constantDelay[IO](1.second))(t => path / file.rotate(t)))
      .compile
      .drain
      .unsafeRunSync()
    val size = Stream.force(hdp.filesIn(path).map(avro.source)).compile.toList.map(_.size).unsafeRunSync()
    assert(size == number * 2)
  }
}
