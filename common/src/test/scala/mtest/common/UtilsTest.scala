package mtest.common

import com.github.chenharryhua.nanjin.common.utils
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime

class UtilsTest extends AnyFunSuite {

  test("1.toProperties - converts map entries correctly") {
    val map = Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3")
    val props = utils.toProperties(map)
    assert(props.getProperty("key1") == "value1")
    assert(props.getProperty("key2") == "value2")
    assert(props.getProperty("key3") == "value3")
    assert(props.size() == 3)
  }

  test("2.toProperties - empty map yields empty properties") {
    val props = utils.toProperties(Map.empty)
    assert(props.isEmpty)
  }

  test("3.toProperties - overwrites duplicate keys with last value") {
    // Map already deduplicates keys, so this is really just confirming Map behavior propagates
    val map = Map("k" -> "v1") ++ Map("k" -> "v2")
    val props = utils.toProperties(map)
    assert(props.getProperty("k") == "v2")
  }

  test("4.random4d - produces values in [1000, 9999]") {
    // Eval.always should produce different values on repeated calls
    val samples = (1 to 100).map(_ => utils.random4d.value)
    assert(samples.forall(n => n >= 1000 && n <= 9999))
  }

  test("5.random4d - is non-memoized (produces varying results)") {
    val samples = (1 to 50).map(_ => utils.random4d.value).toSet
    // With 50 samples from a 9000-range, probability of all being the same is negligible
    assert(samples.size > 1)
  }

  test("6.epoch constants have expected values") {
    assert(utils.epoch == LocalDateTime.of(2019, 7, 21, 0, 0, 0))
    assert(utils.kafkaEpoch == LocalDateTime.of(2012, 10, 23, 0, 0, 0))
    assert(utils.sparkEpoch == LocalDateTime.of(2014, 2, 1, 0, 0, 0))
    assert(utils.flinkEpoch == LocalDateTime.of(2014, 12, 1, 0, 0, 0))
  }
}
