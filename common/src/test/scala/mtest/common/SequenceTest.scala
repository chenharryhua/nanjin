package mtest.common

import com.github.chenharryhua.nanjin.common.sequence.*
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.TimeUnit.{MILLISECONDS, SECONDS}
import scala.concurrent.duration.FiniteDuration

class SequenceTest extends AnyFunSuite {

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
    assertThrows[IllegalArgumentException] {
      exponential(SECONDS).take(60).toList
    }
  }
}
