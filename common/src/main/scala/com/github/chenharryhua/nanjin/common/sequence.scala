package com.github.chenharryhua.nanjin.common

import scala.concurrent.duration.{FiniteDuration, TimeUnit}

/** Infinite lazy numeric sequences and their `FiniteDuration` views.
  *
  * Each sequence is an infinite `LazyList`; take a finite prefix (e.g. `fibonacci.take(8)`) before forcing
  * it. The `(tu: TimeUnit)` overloads reinterpret each element as a `FiniteDuration` in the given unit, which
  * is handy for building retry/backoff schedules such as `Policy.fixedDelay(fibonacci(SECONDS).take(5))`.
  *
  * ==Caveat: `FiniteDuration` range==
  * A `FiniteDuration` holds at most `Long.MaxValue` nanoseconds (~106,751 days). For coarse units the numeric
  * value can exceed that bound; forcing such an element throws `IllegalArgumentException`. `exponential`
  * reaches the bound fastest (and its values also overflow to negative around the 63rd element). Keep the
  * taken prefix small, especially with large time units.
  */
object sequence {

  /** The Fibonacci numbers 1, 1, 2, 3, 5, 8, ... as an infinite lazy sequence. */
  val fibonacci: LazyList[Long] =
    1L #:: 1L #:: fibonacci.zip(fibonacci.tail).map { case (a, b) => a + b }

  /** `fibonacci` reinterpreted as durations in `tu`. See the range caveat on `sequence`. */
  def fibonacci(tu: TimeUnit): LazyList[FiniteDuration] =
    fibonacci.map(v => FiniteDuration(v, tu))

  /** Powers of two 1, 2, 4, 8, ... (`1L << x`) as an infinite lazy sequence.
    *
    * Note: values overflow to negative once the shift reaches 63, so only a modest prefix is meaningful.
    */
  val exponential: LazyList[Long] =
    LazyList.from(0).map(x => 1L << x)

  /** `exponential` reinterpreted as durations in `tu`. Reaches the `FiniteDuration` bound quickly; see the
    * range caveat on `sequence`.
    */
  def exponential(tu: TimeUnit): LazyList[FiniteDuration] =
    exponential.map(v => FiniteDuration(v, tu))

  /** The prime numbers 2, 3, 5, 7, 11, ... as an infinite lazy sequence, built by trial division against the
    * primes already found (checking divisors up to the square root).
    */
  val primes: LazyList[Long] =
    2L #:: LazyList.iterate(3L)(_ + 1L).filter(i =>
      primes.takeWhile(p => (p * p) <= i).forall(p => (i % p) > 0))

  /** `primes` reinterpreted as durations in `tu`. See the range caveat on `sequence`. */
  def primes(tu: TimeUnit): LazyList[FiniteDuration] =
    primes.map(v => FiniteDuration(v, tu))
}
