package mtest.common

import com.github.chenharryhua.nanjin.common.sequence.*
import munit.FunSuite

import java.util.concurrent.TimeUnit.{MILLISECONDS, SECONDS}
import scala.concurrent.duration.FiniteDuration

class SequenceTest extends FunSuite {

  test("1.fibonacci values") {
    assert(fibonacci.take(10).toList == List(1L, 1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L))
  }

  test("2.exponential values") {
    assert(exponential.take(10).toList == List(1L, 2L, 4L, 8L, 16L, 32L, 64L, 128L, 256L, 512L))
  }

  test("3.primes values") {
    assert(primes.take(10).toList == List(2L, 3L, 5L, 7L, 11L, 13L, 17L, 19L, 23L, 29L))
  }

  test("4.fibonacci as durations carries value and unit") {
    val ds = fibonacci(SECONDS).take(5).toList
    assert(
      ds == List(
        FiniteDuration(1L, SECONDS),
        FiniteDuration(1L, SECONDS),
        FiniteDuration(2L, SECONDS),
        FiniteDuration(3L, SECONDS),
        FiniteDuration(5L, SECONDS)))
    assert(ds.forall(_.unit == SECONDS))
  }

  test("5.exponential as durations carries value and unit") {
    val ds = exponential(MILLISECONDS).take(5).toList
    assert(ds.map(_.length) == List(1L, 2L, 4L, 8L, 16L))
    assert(ds.forall(_.unit == MILLISECONDS))
  }

  test("6.primes as durations carries value and unit") {
    val ds = primes(MILLISECONDS).take(5).toList
    assert(ds.map(_.length) == List(2L, 3L, 5L, 7L, 11L))
    assert(ds.forall(_.unit == MILLISECONDS))
  }

  test("7.duration overloads share the underlying numeric prefix") {
    assert(fibonacci(SECONDS).take(6).map(_.length).toList == fibonacci.take(6).toList)
    assert(primes(SECONDS).take(6).map(_.length).toList == primes.take(6).toList)
  }

  test("8.exponential in seconds eventually exceeds FiniteDuration range") {
    // FiniteDuration holds at most Long.MaxValue nanoseconds; a large power of two in SECONDS overflows it.
    intercept[IllegalArgumentException] {
      exponential(SECONDS).take(60).toList
    }
  }

  test("9.exponential doubles up to 2^62 and then holds that value") {
    val maxPowerOfTwo = 1L << 62
    assertEquals(exponential(62), maxPowerOfTwo)
    assertEquals(exponential(61), maxPowerOfTwo / 2)
    // a plain `1L << x` wrapped here: element 63 was Long.MinValue and element 64 was back to 1
    assertEquals(exponential(63), maxPowerOfTwo)
    assertEquals(exponential(64), maxPowerOfTwo)
    assertEquals(exponential(500), maxPowerOfTwo)
  }

  test("10.exponential stays positive, powers of two, and non-decreasing") {
    val prefix = exponential.take(200).toList
    assert(prefix.forall(_ > 0L), "no element may overflow to a negative value")
    // a positive power of two has exactly one bit set
    assert(prefix.forall(v => java.lang.Long.bitCount(v) == 1), "every element must be a power of two")
    assert(
      prefix.zip(prefix.tail).forall { case (a, b) => a <= b },
      "the sequence must never collapse back to smaller delays")
  }

  test("11.fibonacci exceeds Long after its first 92 elements") {
    // Unlike exponential, fibonacci does not saturate. This pins where it stops being usable so the
    // documented limit cannot drift.
    val valid = fibonacci.take(92).toList
    assert(valid.forall(_ > 0L))
    assert(valid.zip(valid.tail).forall { case (a, b) => a <= b })
    assert(fibonacci(92) < 0L, "F(93) overflows Long and wraps negative")
  }
}
