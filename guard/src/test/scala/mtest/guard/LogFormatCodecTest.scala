package mtest.guard

import com.github.chenharryhua.nanjin.guard.config.LogFormat
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

class LogFormatCodecTest extends AnyFunSuite {

  test("1.all LogFormat values round-trip through JSON") {
    LogFormat.values.foreach { lf =>
      val json = lf.asJson
      val decoded = decode[LogFormat](json.noSpaces)
      assert(decoded == Right(lf), s"Failed round-trip for $lf")
    }
  }

  test("2.LogFormat encodes to string matching productPrefix") {
    assert(LogFormat.ConsolePlainText.asJson.asString.contains("ConsolePlainText"))
    assert(LogFormat.ConsoleJson.asJson.asString.contains("ConsoleJson"))
    assert(LogFormat.ConsoleJsonMultiLine.asJson.asString.contains("ConsoleJsonMultiLine"))
    assert(LogFormat.ConsoleJsonVerbose.asJson.asString.contains("ConsoleJsonVerbose"))
    assert(LogFormat.Slf4jJson.asJson.asString.contains("Slf4jJson"))
  }

  test("3.invalid string produces decoding failure") {
    val result = decode[LogFormat](""""InvalidFormat"""")
    assert(result.isLeft)
  }

  test("4.all enum variants are covered (5 total)") {
    assert(LogFormat.values.length == 5)
  }
}
