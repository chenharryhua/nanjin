package mtest.common

import com.github.chenharryhua.nanjin.common.NJLogLevel
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

class NJLogLevelTest extends AnyFunSuite {

  test("should be compared: all < off") {
    import cats.syntax.order.*
    val all: NJLogLevel = NJLogLevel.ALL
    val off: NJLogLevel = NJLogLevel.OFF
    assert(all < off)
  }

  test("should be compared: info == info") {
    assert(NJLogLevel.INFO === NJLogLevel.INFO)
  }

  test("json") {
    val l1: NJLogLevel = NJLogLevel.ALL
    val l2: NJLogLevel = NJLogLevel.TRACE
    val l3: NJLogLevel = NJLogLevel.DEBUG
    val l4: NJLogLevel = NJLogLevel.INFO
    val l5: NJLogLevel = NJLogLevel.WARN
    val l6: NJLogLevel = NJLogLevel.ERROR
    val l7: NJLogLevel = NJLogLevel.FATAL
    val l8: NJLogLevel = NJLogLevel.OFF

    assert(l1.asJson.noSpaces === """ "all" """.trim)
    assert(l2.asJson.noSpaces === """ "trace" """.trim)
    assert(l3.asJson.noSpaces === """ "debug" """.trim)
    assert(l4.asJson.noSpaces === """ "info" """.trim)
    assert(l5.asJson.noSpaces === """ "warn" """.trim)
    assert(l6.asJson.noSpaces === """ "error" """.trim)
    assert(l7.asJson.noSpaces === """ "fatal" """.trim)
    assert(l8.asJson.noSpaces === """ "off" """.trim)
    assert(l1.productPrefix === "ALL")
  }
}
