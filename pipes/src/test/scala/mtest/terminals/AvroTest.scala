package mtest.terminals

import akka.stream.scaladsl.Source
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.scalatest.funsuite.AnyFunSuite

class AvroTest extends AnyFunSuite {
  import HadoopTestData.*
  test("snappy avro write/read") {
    val pathStr = NJPath("./data/test/devices/panda.snappy.avro")
    hdp.delete(pathStr).unsafeRunSync()
    val sink = hdp.avro(pandaSchema).withCodecFactory(CodecFactory.snappyCodec).sink(pathStr)
    val src  = hdp.avro(pandaSchema).withCodecFactory(CodecFactory.snappyCodec).source(pathStr)
    val ts   = Stream.emits(pandas).covary[IO]
    ts.through(sink).compile.drain.unsafeRunSync()
    val action = src.compile.toList
    assert(action.unsafeRunSync() == pandas)
  }

  test("deflate(6) avro write/read") {
    val pathStr = NJPath("./data/test/devices/panda.deflate.avro")
    hdp.delete(pathStr).unsafeRunSync()
    val ts   = Stream.emits(pandas).covary[IO]
    val sink = hdp.avro(pandaSchema).withCodecFactory(CodecFactory.deflateCodec(6)).sink(pathStr)
    val src  = hdp.avro(pandaSchema).source(pathStr)
    ts.through(sink).compile.drain.unsafeRunSync()
    val action = src.compile.toList
    assert(action.unsafeRunSync() == pandas)
  }

  test("uncompressed avro write/read") {
    val pathStr = NJPath("./data/test/devices/panda.uncompressed.avro")
    hdp.delete(pathStr).unsafeRunSync()
    val ts = Stream.emits(pandas).covary[IO]
    ts.through(hdp.avro(pandaSchema).sink(pathStr)).compile.drain.unsafeRunSync()
    val action = hdp.avro(pandaSchema).source(pathStr).compile.toList
    assert(action.unsafeRunSync() == pandas)
  }

  test("uncompressed avro write/read akka") {
    val pathStr = NJPath("./data/test/devices/akka/panda.uncompressed.avro")
    hdp.delete(pathStr).unsafeRunSync()

    val ts   = Source(pandas)
    val avro = hdp.avro(pandaSchema).withCodecFactory(CodecFactory.nullCodec())
    val sink = avro.akka.sink(pathStr)
    val src  = avro.akka.source(pathStr)

    val action =
      IO.fromFuture(IO(ts.runWith(sink))) >>
        IO.fromFuture(IO(src.runFold(List.empty[GenericRecord])(_.appended(_))))

    assert(action.unsafeRunSync() == pandas)
  }
}
