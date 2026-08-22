package mtest.guard

import com.github.chenharryhua.nanjin.guard.config.StackTrace
import com.github.chenharryhua.nanjin.guard.event.StopReason
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

class StopReasonCodecTest extends AnyFunSuite {

  test("1.Successfully round-trip") {
    val json = StopReason.Successfully.asJson
    assert(json.asString.contains("Successfully"))
    assert(decode[StopReason](json.noSpaces) == Right(StopReason.Successfully))
  }

  test("2.ByCancellation round-trip") {
    val json = StopReason.ByCancellation.asJson
    assert(json.asString.contains("ByCancellation"))
    assert(decode[StopReason](json.noSpaces) == Right(StopReason.ByCancellation))
  }

  test("3.Maintenance round-trip") {
    val json = StopReason.Maintenance.asJson
    assert(json.asString.contains("Maintenance"))
    assert(decode[StopReason](json.noSpaces) == Right(StopReason.Maintenance))
  }

  test("4.ByException round-trip") {
    val ex = new RuntimeException("test error\nwith newline")
    val stackTrace = StackTrace(ex)
    val reason = StopReason.ByException(stackTrace)
    val json = reason.asJson
    val decoded = decode[StopReason](json.noSpaces)
    assert(decoded.isRight)
    decoded match {
      case Right(StopReason.ByException(st)) =>
        assert(st.value.nonEmpty)
        assert(st.value.head.contains("test error"))
      case other => fail(s"expected ByException but got $other")
    }
  }

  test("5.ByException with nested cause round-trip") {
    val cause = new IllegalStateException("root cause")
    val ex = new RuntimeException("wrapper", cause)
    val stackTrace = StackTrace(ex)
    val reason = StopReason.ByException(stackTrace)
    val json = reason.asJson
    val decoded = decode[StopReason](json.noSpaces)
    assert(decoded.isRight)
    decoded.foreach {
      case StopReason.ByException(st) =>
        // should contain root cause info
        assert(st.value.exists(_.contains("root cause")))
      case _ => fail("wrong variant")
    }
  }

  test("6.unrecognized string produces DecodingFailure") {
    val result = decode[StopReason](""""UnknownReason"""")
    assert(result.isLeft)
  }

  test("7.exitCode values are correct") {
    assert(StopReason.Successfully.exitCode == 0)
    assert(StopReason.Maintenance.exitCode == 1)
    assert(StopReason.ByCancellation.exitCode == 2)
    assert(StopReason.ByException(StackTrace(new Exception)).exitCode == 3)
  }
}
