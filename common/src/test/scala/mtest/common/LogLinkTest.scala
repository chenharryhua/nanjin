package mtest.common

import com.github.chenharryhua.nanjin.common.logging.LogLink
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Json}
import munit.FunSuite

class LogLinkTest extends FunSuite {

  private val raw = "https://console.example.com/logs?group=svc&t=42"

  test("1.value returns the underlying string") {
    assertEquals(LogLink(raw).value, raw)
  }

  test("2.encoder emits the link as a JSON string") {
    assertEquals(LogLink(raw).asJson, Json.fromString(raw))
  }

  test("3.encode/decode round-trips a link") {
    val decoded = Decoder[LogLink].decodeJson(LogLink(raw).asJson)
    assertEquals(decoded.map(_.value), Right(raw))
  }
}
