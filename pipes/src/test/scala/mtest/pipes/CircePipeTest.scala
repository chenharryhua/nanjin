package mtest.pipes

import akka.stream.scaladsl.Source
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.CirceSerde
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.generic.auto.*
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class CircePipeTest extends AnyFunSuite {
  import TestData.*
  val data: Stream[IO, Tiger] = Stream.emits(tigers)
  val hd: NJHadoop[IO]        = NJHadoop[IO](new Configuration)

  test("circe identity - remove null") {
    assert(
      data
        .through(CirceSerde.toBytes[IO, Tiger](isKeepNull = false))
        .through(CirceSerde.fromBytes[IO, Tiger])
        .compile
        .toList
        .unsafeRunSync() === tigers)
  }

  test("circe identity - keep null") {
    assert(
      data
        .through(CirceSerde.toBytes[IO, Tiger](isKeepNull = true))
        .through(CirceSerde.fromBytes[IO, Tiger])
        .compile
        .toList
        .unsafeRunSync() === tigers)
  }

  test("circe identity - remove null akka") {
    import mtest.terminals.mat
    val rst = IO.fromFuture(
      IO(
        Source(tigers)
          .via(CirceSerde.akka.toByteString(isKeepNull = false))
          .via(CirceSerde.akka.fromByteString[Tiger])
          .runFold(List.empty[Tiger]) { case (ss, i) => ss :+ i }))

    assert(rst.unsafeRunSync() === tigers)
  }

  test("circe identity - keep null akka") {
    import mtest.terminals.*
    val src = Source(tigers)
    val rst = IO.fromFuture(
      IO(
        src
          .via(CirceSerde.akka.toByteString(isKeepNull = true))
          .via(CirceSerde.akka.fromByteString[Tiger])
          .runFold(List.empty[Tiger]) { case (ss, i) => ss :+ i }))

    assert(rst.unsafeRunSync() === tigers)
  }

  test("read/write test uncompressed") {
    val path = NJPath("data/pipe/circe.json")
    hd.delete(path).unsafeRunSync()
    val rst = hd.bytes.source(path).through(CirceSerde.fromBytes[IO, Tiger]).compile.toList
    data.through(CirceSerde.toBytes(true)).through(hd.bytes.sink(path)).compile.drain.unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write test gzip") {
    val path = NJPath("data/pipe/circe.json.gz")
    hd.delete(path).unsafeRunSync()
    val rst = hd.bytes.source(path).through(CirceSerde.fromBytes[IO, Tiger]).compile.toList
    data.through(CirceSerde.toBytes(true)).through(hd.bytes.sink(path)).compile.drain.unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write test snappy") {
    val path = NJPath("data/pipe/circe.json.snappy")
    hd.delete(path).unsafeRunSync()
    val rst = hd.bytes.source(path).through(CirceSerde.fromBytes[IO, Tiger]).compile.toList
    data.through(CirceSerde.toBytes(true)).through(hd.bytes.sink(path)).compile.drain.unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write bzip2") {
    val path = NJPath("data/pipe/circe.json.bz2")
    hd.delete(path).unsafeRunSync()
    val rst = hd.bytes.source(path).through(CirceSerde.fromBytes[IO, Tiger]).compile.toList
    data.through(CirceSerde.toBytes(true)).through(hd.bytes.sink(path)).compile.drain.unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write lz4") {
    val path = NJPath("data/pipe/circe.json.lz4")
    hd.delete(path).unsafeRunSync()
    val rst = hd.bytes.source(path).through(CirceSerde.fromBytes[IO, Tiger]).compile.toList
    data.through(CirceSerde.toBytes(true)).through(hd.bytes.sink(path)).compile.drain.unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }

  test("read/write deflate") {
    val path = NJPath("data/pipe/circe.json.deflate")
    hd.delete(path).unsafeRunSync()
    val rst = hd.bytes.source(path).through(CirceSerde.fromBytes[IO, Tiger]).compile.toList
    data.through(CirceSerde.toBytes(true)).through(hd.bytes.sink(path)).compile.drain.unsafeRunSync()
    assert(rst.unsafeRunSync() == tigers)
  }
}
