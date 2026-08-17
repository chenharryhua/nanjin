package com.github.chenharryhua.nanjin.common

object sequence {
  val fibonacci: LazyList[Long] =
    1L #:: 1L #:: fibonacci.zip(fibonacci.tail).map { case (a, b) => a + b }

  val exponential: LazyList[Long] =
    LazyList.from(0).map(x => 1L << x)

  val primes: LazyList[Int] =
    2 #:: LazyList.from(3).filter(i => primes.takeWhile(p => (p * p) <= i).forall(p => (i % p) > 0))
}
