package mtest.common

import com.github.chenharryhua.nanjin.common.NJLogLevel
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.log4cats.extras.LogLevel

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

    assert(l1.logLevel === LogLevel.Trace)
    assert(l2.logLevel === LogLevel.Trace)
    assert(l3.asJson.noSpaces === """ 3 """.trim)
    assert(l4.asJson.noSpaces === """ 4 """.trim)
    assert(l5.asJson.noSpaces === """ 5 """.trim)
    assert(l6.asJson.noSpaces === """ 6 """.trim)
    assert(l7.asJson.noSpaces === """ 7 """.trim)
    assert(l8.asJson.noSpaces === """ 8 """.trim)
    assert(l1.productPrefix === "ALL")
  }
}
