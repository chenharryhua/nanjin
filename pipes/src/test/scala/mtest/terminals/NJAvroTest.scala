package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.{NJAvro, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import scala.concurrent.duration.DurationInt

class NJAvroTest extends AnyFunSuite {
  import HadoopTestData.*

  val avro: NJAvro[IO] = hdp.avro(pandaSchema)

  def fs2(path: NJPath, codecFactory: CodecFactory, data: Set[GenericRecord]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val sink   = avro.withChunkSize(100).withBlockSizeHint(1000).withCodecFactory(codecFactory).sink(path)
    val src    = avro.source(path)
    val ts     = Stream.emits(data.toList).covary[IO]
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/avro/fs2")

  test("snappy avro") {
    fs2(fs2Root / "panda.snappy.avro", CodecFactory.snappyCodec, pandaSet)
  }

  test("deflate 6 avro") {
    fs2(fs2Root / "panda.deflate.avro", CodecFactory.deflateCodec(6), pandaSet)
  }

  test("uncompressed avro") {
    fs2(fs2Root / "panda.uncompressed.avro", CodecFactory.nullCodec(), pandaSet)
  }

  test("xz 1 avro") {
    fs2(fs2Root / "panda.xz.avro", CodecFactory.xzCodec(1), pandaSet)
  }

  test("bzip2 avro") {
    fs2(fs2Root / "panda.bz2.avro", CodecFactory.bzip2Codec(), pandaSet)
  }

  test("zstandard avro") {
    fs2(fs2Root / "panda.zstandard.avro", CodecFactory.zstandardCodec(1), pandaSet)
  }

  test("laziness") {
    avro.source(NJPath("./does/not/exist"))
    avro.sink(NJPath("./does/not/exist"))
  }

  test("rotation") {
    val path = fs2Root / "rotation"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(avro.sink(RetryPolicies.constantDelay[IO](1.second))(t => path / s"${t.index}.avro"))
      .compile
      .drain
      .unsafeRunSync()
    val size = Stream.force(hdp.filesIn(path).map(avro.source)).compile.toList.map(_.size).unsafeRunSync()
    assert(size == number * 2)
  }
}
