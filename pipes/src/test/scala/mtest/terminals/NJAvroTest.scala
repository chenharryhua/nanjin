package mtest.terminals

import akka.stream.scaladsl.Source
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.{NJAvro, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

class NJAvroTest extends AnyFunSuite {
  import HadoopTestData.*

  val avro: NJAvro[IO] = hdp.avro(pandaSchema)

  def fs2(path: NJPath, codecFactory: CodecFactory, data: Set[GenericRecord]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val sink   = avro.withCodecFactory(codecFactory).sink(path)
    val src    = avro.source(path)
    val ts     = Stream.emits(data.toList).covary[IO]
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  def akka(path: NJPath, codecFactory: CodecFactory, data: Set[GenericRecord]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val ts   = Source(data)
    val sink = avro.withCodecFactory(codecFactory).akka.sink(path)
    val src  = avro.akka.source(path)
    val action = IO.fromFuture(IO(ts.runWith(sink))) >>
      IO.fromFuture(IO(src.runFold(Set.empty[GenericRecord])(_ + _)))

    assert(action.unsafeRunSync().toSet == data)
  }

  val akkaRoot: NJPath = NJPath("./data/test/terminals/avro/akka")
  val fs2Root: NJPath  = NJPath("./data/test/terminals/avro/fs2")

  test("snappy avro") {
    fs2(fs2Root / "panda.snappy.avro", CodecFactory.snappyCodec, pandaSet)
    akka(akkaRoot / "panda.snappy.avro", CodecFactory.snappyCodec, pandaSet)
  }

  test("deflate 6 avro") {
    fs2(fs2Root / "panda.deflate.avro", CodecFactory.deflateCodec(6), pandaSet)
    akka(akkaRoot / "panda.deflate.avro", CodecFactory.deflateCodec(6), pandaSet)
  }

  test("uncompressed avro") {
    fs2(fs2Root / "panda.uncompressed.avro", CodecFactory.nullCodec(), pandaSet)
    akka(akkaRoot / "panda.uncompressed.avro", CodecFactory.nullCodec(), pandaSet)
  }

  test("xz 1 avro") {
    fs2(fs2Root / "panda.xz.avro", CodecFactory.xzCodec(1), pandaSet)
    akka(akkaRoot / "panda.xz.avro", CodecFactory.xzCodec(1), pandaSet)
  }

  test("bzip2 avro") {
    fs2(fs2Root / "panda.bz2.avro", CodecFactory.bzip2Codec(), pandaSet)
    akka(akkaRoot / "panda.bz2.avro", CodecFactory.bzip2Codec, pandaSet)
  }

  test("zstandard avro") {
    fs2(fs2Root / "panda.zstandard.avro", CodecFactory.zstandardCodec(1), pandaSet)
    akka(akkaRoot / "panda.zstandard.avro", CodecFactory.zstandardCodec(1), pandaSet)
  }
}
