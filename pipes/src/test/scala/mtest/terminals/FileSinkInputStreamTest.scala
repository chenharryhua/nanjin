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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

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

  test("inputStream honors the buffer size in bytes for non-byte units") {
    val path = Url.parse("./data/test/terminals/input-stream/buffer-size.bin")
    val maxLen = new AtomicInteger(0)
    // payload larger than one byte so the requested read length is observable
    val payload = Array.fill[Byte](4096)(7)
    val input = new InputStream {
      private val delegate = new ByteArrayInputStream(payload)

      override def read(): Int = delegate.read()

      override def read(b: Array[Byte], off: Int, len: Int): Int = {
        maxLen.updateAndGet(m => math.max(m, len))
        delegate.read(b, off, len)
      }
    }

    hdp.delete(path).unsafeRunSync()
    Stream
      .emit(input)
      .covary[IO]
      .through(hdp.sink(path).inputStream(1.kb)) // 1.kb == 1000 bytes
      .compile
      .drain
      .unsafeRunSync()

    // With the old `bufferSize.value.toInt`, Kilobytes(1).value == 1.0 -> a 1-byte buffer.
    // With `bufferSize.toBytes.toInt`, the buffer is the intended 1000 bytes.
    assert(maxLen.get() == 1000, s"expected read buffer size 1000, got ${maxLen.get()}")
  }
}
