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
    val hd   = NJHadoop[IO](new Configuration())
    val path = NJPath("data/pipe/circe.json")
    val rst  = hd.byteSource(path).through(CirceSerde.deserPipe[IO, Tiger]).compile.toList
    data.through(CirceSerde.serPipe(true)).through(hd.byteSink(path)).compile.drain.unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write test gzip") {
    val hd   = NJHadoop[IO](new Configuration())
    val path = NJPath("data/pipe/circe.json.gz")
    val rst  = hd.byteSource(path).through(CirceSerde.deserPipe[IO, Tiger]).compile.toList
    data
      .through(CirceSerde.serPipe(true))
      .through(hd.byteSink(path, Some(new GzipCodec())))
      .compile
      .drain
      .unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write test snappy") {
    val hd   = NJHadoop[IO](new Configuration())
    val path = NJPath("data/pipe/circe.json.snappy")
    val rst  = hd.byteSource(path).through(CirceSerde.deserPipe[IO, Tiger]).compile.toList
    data
      .through(CirceSerde.serPipe(true))
      .through(hd.byteSink(path, Some(new SnappyCodec())))
      .compile
      .drain
      .unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write bzip2") {
    val hd   = NJHadoop[IO](new Configuration())
    val path = NJPath("data/pipe/circe.json.bz2")
    val rst  = hd.byteSource(path).through(CirceSerde.deserPipe[IO, Tiger]).compile.toList
    data
      .through(CirceSerde.serPipe(true))
      .through(hd.byteSink(path, Some(new BZip2Codec())))
      .compile
      .drain
      .unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write lz4") {
    val hd   = NJHadoop[IO](new Configuration())
    val path = NJPath("data/pipe/circe.json.lz4")
    val rst  = hd.byteSource(path).through(CirceSerde.deserPipe[IO, Tiger]).compile.toList
    data
      .through(CirceSerde.serPipe(true))
      .through(hd.byteSink(path, Some(new Lz4Codec())))
      .compile
      .drain
      .unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write deflate") {
    val hd   = NJHadoop[IO](new Configuration())
    val path = NJPath("data/pipe/circe.json.deflate")
    val rst  = hd.byteSource(path).through(CirceSerde.deserPipe[IO, Tiger]).compile.toList
    data
      .through(CirceSerde.serPipe(true))
      .through(hd.byteSink(path, Some(new DeflateCodec())))
      .compile
      .drain
      .unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }
}
