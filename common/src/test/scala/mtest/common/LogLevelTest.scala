package mtest.common

import cats.Order
import cats.syntax.order.given
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import io.circe.Decoder
import io.circe.syntax.EncoderOps
import munit.FunSuite

class LogLevelTest extends FunSuite {

  private val levels: List[LogLevel] =
    List(LogLevel.Error, LogLevel.Warn, LogLevel.Good, LogLevel.Info, LogLevel.Debug)

  test("1.encoder renders each level as its product prefix") {
    assertEquals(LogLevel.Error.asJson, io.circe.Json.fromString("Error"))
    assertEquals(LogLevel.Warn.asJson, io.circe.Json.fromString("Warn"))
    assertEquals(LogLevel.Good.asJson, io.circe.Json.fromString("Good"))
    assertEquals(LogLevel.Info.asJson, io.circe.Json.fromString("Info"))
    assertEquals(LogLevel.Debug.asJson, io.circe.Json.fromString("Debug"))
  }

  test("2.encode/decode round-trips every level") {
    levels.foreach { level =>
      val decoded = Decoder[LogLevel].decodeJson(level.asJson)
      assertEquals(decoded, Right(level))
    }
  }

  test("3.decoder rejects an unknown name") {
    val decoded = Decoder[LogLevel].decodeJson(io.circe.Json.fromString("Trace"))
    assert(decoded.isLeft)
    assert(decoded.swap.toOption.exists(_.getMessage.contains("Invalid LogLevel")))
  }

  test("4.show renders the product prefix") {
    assertEquals(LogLevel.Error.show, "Error")
    assertEquals(LogLevel.Debug.show, "Debug")
  }

  test("5.order ranks Error above Warn above Good above Info above Debug") {
    assert(LogLevel.Error > LogLevel.Warn)
    assert(LogLevel.Warn > LogLevel.Good)
    assert(LogLevel.Good > LogLevel.Info)
    assert(LogLevel.Info > LogLevel.Debug)
    assertEquals(levels.sorted(using Order[LogLevel].toOrdering), levels.reverse)
  }
}
