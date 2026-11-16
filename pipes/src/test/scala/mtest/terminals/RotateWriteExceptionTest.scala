package mtest.terminals

import com.github.chenharryhua.nanjin.terminals.{CreateRotateFile, RotateWriteException}
import io.lemonlabs.uri.Url
import munit.FunSuite

import java.time.{ZoneId, ZonedDateTime}
import java.util.UUID

class RotateWriteExceptionTest extends FunSuite {

  private val CREATE = CreateRotateFile(
    sequenceId = UUID.fromString("12345678-90ab-cdef-1234-567890abcdef"),
    index = 7L,
    time = ZonedDateTime.of(2026, 1, 2, 3, 4, 5, 0, ZoneId.of("UTC")))
  private val URL = Url.parse("s3a://bucket/output/0007.data.avro")

  test("1.fields and cause preserve the failing rotation context") {
    val cause = new RuntimeException("disk full")
    val error = RotateWriteException(CREATE, URL, offset = 42L, cause)

    assertEquals(error.create, CREATE)
    assertEquals(error.url, URL)
    assertEquals(error.offset, 42L)
    assert(error.cause eq cause)
    assert(error.getCause eq cause)
  }

  test("2.message identifies URL, per-file offset, index, and creation time") {
    val error = RotateWriteException(CREATE, URL, offset = 42L, new RuntimeException("disk full"))

    assert(error.getMessage.contains(URL.toString), error.getMessage)
    assert(error.getMessage.contains("offset=42"), error.getMessage)
    assert(error.getMessage.contains("index=7"), error.getMessage)
    assert(error.getMessage.contains(CREATE.time.toString), error.getMessage)
  }

  test("3.zero and large offsets are rendered without reinterpretation") {
    val cause = new RuntimeException("failure")
    val zero = RotateWriteException(CREATE, URL, 0L, cause)
    val large = RotateWriteException(CREATE, URL, Long.MaxValue, cause)

    assert(zero.getMessage.contains("offset=0"), zero.getMessage)
    assert(large.getMessage.contains(s"offset=${Long.MaxValue}"), large.getMessage)
  }
}
