package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.CirceSerde
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.generic.auto.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.*
import org.scalatest.funsuite.AnyFunSuite

class CircePipeTest extends AnyFunSuite {
  import TestData.*
  val data: Stream[IO, Tiger] = Stream.emits(tigers)
  val hd: NJHadoop[IO]        = NJHadoop[IO](new Configuration())

  test("circe identity - remove null") {
    assert(
      data
        .through(CirceSerde.serPipe[IO, Tiger](isKeepNull = false))
        .through(CirceSerde.deserPipe[IO, Tiger])
        .compile
        .toList
        .unsafeRunSync() === tigers)
  }

  test("circe identity - keep null") {
    assert(
      data
        .through(CirceSerde.serPipe[IO, Tiger](isKeepNull = true))
        .through(CirceSerde.deserPipe[IO, Tiger])
        .compile
        .toList
        .unsafeRunSync() === tigers)
  }

  test("read/write test uncompressed") {
    val path = NJPath("data/pipe/circe.json")
    hd.delete(path).unsafeRunSync()
    val rst = hd.bytes.source(path).through(CirceSerde.deserPipe[IO, Tiger]).compile.toList
    data.through(CirceSerde.serPipe(true)).through(hd.bytes.sink(path)).compile.drain.unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write test gzip") {
    val path = NJPath("data/pipe/circe.json.gz")
    hd.delete(path).unsafeRunSync()
    val rst = hd.bytes.source(path).through(CirceSerde.deserPipe[IO, Tiger]).compile.toList
    data
      .through(CirceSerde.serPipe(true))
      .through(hd.bytes.withCompressionCodec(new GzipCodec).sink(path))
      .compile
      .drain
      .unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write test snappy") {
    val path = NJPath("data/pipe/circe.json.snappy")
    hd.delete(path).unsafeRunSync()
    val rst = hd.bytes.source(path).through(CirceSerde.deserPipe[IO, Tiger]).compile.toList
    data
      .through(CirceSerde.serPipe(true))
      .through(hd.bytes.withCompressionCodec(new SnappyCodec()).sink(path))
      .compile
      .drain
      .unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write bzip2") {
    val path = NJPath("data/pipe/circe.json.bz2")
    hd.delete(path).unsafeRunSync()
    val rst = hd.bytes.source(path).through(CirceSerde.deserPipe[IO, Tiger]).compile.toList
    data
      .through(CirceSerde.serPipe(true))
      .through(hd.bytes.withCompressionCodec(new BZip2Codec).sink(path))
      .compile
      .drain
      .unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write lz4") {
    val path = NJPath("data/pipe/circe.json.lz4")
    hd.delete(path).unsafeRunSync()
    val rst = hd.bytes.source(path).through(CirceSerde.deserPipe[IO, Tiger]).compile.toList
    data
      .through(CirceSerde.serPipe(true))
      .through(hd.bytes.withCompressionCodec(new Lz4Codec).sink(path))
      .compile
      .drain
      .unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write deflate") {
    val path = NJPath("data/pipe/circe.json.deflate")
    hd.delete(path).unsafeRunSync()
    val rst = hd.bytes.source(path).through(CirceSerde.deserPipe[IO, Tiger]).compile.toList
    data
      .through(CirceSerde.serPipe(true))
      .through(hd.bytes.withCompressionCodec(new DeflateCodec).sink(path))
      .compile
      .drain
      .unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }
}
