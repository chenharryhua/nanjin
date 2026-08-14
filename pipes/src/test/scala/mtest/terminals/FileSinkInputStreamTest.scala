package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import io.lemonlabs.uri.Url
import mtest.terminals.HadoopTestData.hdp
import org.scalatest.funsuite.AnyFunSuite
import squants.information.Bytes
import squants.information.InformationConversions.InformationConversions

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean

class FileSinkInputStreamTest extends AnyFunSuite {

  test("inputStream writes all input bytes and reports their count") {
    val path = Url.parse("./data/test/terminals/input-stream/bytes.bin")
    val first = "first stream\n".getBytes(StandardCharsets.UTF_8)
    val second = "second stream".getBytes(StandardCharsets.UTF_8)
    val expected = first ++ second

    hdp.delete(path).unsafeRunSync()
    val written = Stream
      .emits(List(new ByteArrayInputStream(first), new ByteArrayInputStream(second)))
      .covary[IO]
      .through(hdp.sink(path).inputStream(3.bytes))
      .compile
      .fold(0)(_ + _)
      .unsafeRunSync()

    val actual = hdp.source(path).bytes(Bytes(2)).compile.to(Array).unsafeRunSync()
    assert(written == expected.length)
    assert(actual.sameElements(expected))
  }

  test("inputStream closes each input stream") {
    val path = Url.parse("./data/test/terminals/input-stream/closed.bin")
    val closed = new AtomicBoolean(false)
    val input = new InputStream {
      private val delegate = new ByteArrayInputStream(Array[Byte](1, 2, 3))

      override def read(): Int = delegate.read()

      override def close(): Unit = {
        closed.set(true)
        delegate.close()
      }
    }

    hdp.delete(path).unsafeRunSync()
    Stream
      .emit(input)
      .covary[IO]
      .through(hdp.sink(path).inputStream(2.bytes))
      .compile
      .drain
      .unsafeRunSync()

    assert(closed.get())
  }
}
