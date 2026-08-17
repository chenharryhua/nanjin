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
    assert(LogFormat.Console_PlainText.asJson.asString.contains("Console_PlainText"))
    assert(LogFormat.Console_Json.asJson.asString.contains("Console_Json"))
    assert(LogFormat.Console_Json_NoColor.asJson.asString.contains("Console_Json_NoColor"))
    assert(LogFormat.Console_Json_MultiLine.asJson.asString.contains("Console_Json_MultiLine"))
    assert(LogFormat.Console_Json_Verbose.asJson.asString.contains("Console_Json_Verbose"))
    assert(LogFormat.Slf4j_PlainText.asJson.asString.contains("Slf4j_PlainText"))
    assert(LogFormat.Slf4j_Json.asJson.asString.contains("Slf4j_Json"))
    assert(LogFormat.Slf4j_Json_NoColor.asJson.asString.contains("Slf4j_Json_NoColor"))
  }

  test("3.invalid string produces decoding failure") {
    val result = decode[LogFormat](""""InvalidFormat"""")
    assert(result.isLeft)
  }

  test("4.all enum variants are covered (8 total)") {
    assert(LogFormat.values.length == 8)
  }
}
