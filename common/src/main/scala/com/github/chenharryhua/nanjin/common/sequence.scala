package com.github.chenharryhua.nanjin.common

import cats.syntax.eq.catsSyntaxEq

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
  * reaches the bound fastest. Keep the taken prefix small, especially with large time units.
  *
  * ==Caveat: `Long` range==
  * `exponential` holds at 2^62 instead of overflowing, so its values stay valid however long a prefix is
  * taken. `fibonacci` and `primes` do '''not''' saturate: `fibonacci` exceeds `Long` after its first 92
  * elements and silently goes negative from there. Only `exponential` is safe to take an arbitrary prefix of.
  *
  * ==Caveat: memoization==
  * These are `val`s, so a `LazyList` memoizes every element ever forced and retains it for the life of the
  * JVM. Forcing a very long prefix therefore costs memory permanently, not just for the duration of the call.
  */
object sequence {

  /** The Fibonacci numbers 1, 1, 2, 3, 5, 8, ... as an infinite lazy sequence. */
  val fibonacci: LazyList[Long] =
    1L #:: 1L #:: fibonacci.zip(fibonacci.tail).map { case (a, b) => a + b }

  /** `fibonacci` reinterpreted as durations in `tu`. See the range caveat on `sequence`. */
  def fibonacci(tu: TimeUnit): LazyList[FiniteDuration] =
    fibonacci.map(v => FiniteDuration(v, tu))

  /** Powers of two 1, 2, 4, 8, ... as an infinite lazy sequence, holding at 2^62 once it gets there.
    *
    * 2^62 is the largest power of two a `Long` can represent, so the sequence stops doubling at that point
    * and repeats it rather than shifting past it. A plain `1L << x` does not degrade gracefully here: the JVM
    * masks a long shift distance to its low six bits, so element 63 came out as `Long.MinValue` and element
    * 64 wrapped back to `1`, making the whole sequence periodic. For the backoff schedules this is built for,
    * that wrap is the harmful case, because delays collapse from astronomical back to a single unit and a
    * retry loop starts hammering again. Holding at the maximum keeps every element a positive power of two
    * and keeps the sequence monotonically non-decreasing.
    *
    * Reaching the cap needs a 62-element prefix, far beyond any sensible backoff schedule, and such a prefix
    * is unusable as a duration anyway for all but the finest units. See the range caveat on `sequence`.
    */
  val exponential: LazyList[Long] =
    LazyList.from(0).map(x => 1L << math.min(x, 62))

  /** `exponential` reinterpreted as durations in `tu`. Reaches the `FiniteDuration` bound quickly; see the
    * range caveat on `sequence`.
    */
  def exponential(tu: TimeUnit): LazyList[FiniteDuration] =
    exponential.map(v => FiniteDuration(v, tu))

  /** The prime numbers 2, 3, 5, 7, 11, ... as an infinite lazy sequence, built by trial division against the
    * primes already found (checking divisors up to the square root).
    */
  val primes: LazyList[Long] =
    2L #:: LazyList.iterate(3L)(_ + 2L).filter(i =>
      primes.takeWhile(p => p <= i / p).forall(p => (i % p) =!= 0))

  /** `primes` reinterpreted as durations in `tu`. See the range caveat on `sequence`. */
  def primes(tu: TimeUnit): LazyList[FiniteDuration] =
    primes.map(v => FiniteDuration(v, tu))
}
