package mtest.common

import com.github.chenharryhua.nanjin.common.NJLogLevel
import org.scalatest.funsuite.AnyFunSuite

class NJLogLevelTest extends AnyFunSuite {
  test("should be compared: off > all") {
    assert(NJLogLevel.OFF > NJLogLevel.ALL)
  }

  test("should be compared: all < off") {
    val all: NJLogLevel = NJLogLevel.ALL
    val off: NJLogLevel = NJLogLevel.OFF
    assert(all < off)
  }
  test("should be compared: info == info") {
    assert(NJLogLevel.INFO === NJLogLevel.INFO)
  }
}
