package com.github.chenharryhua.nanjin.common

import scala.math.pow

object sequence {
  val fibonacci: LazyList[Int] =
    1 #:: 1 #:: fibonacci.zip(fibonacci.tail).map { case (a, b) => a + b }

  val exponential: LazyList[Int] =
    LazyList.from(0).map(x => pow(2, x.toDouble).toInt)

  val primes: LazyList[Int] =
    2 #:: LazyList.from(3).filter(i => primes.takeWhile(p => (p * p) <= i).forall(p => (i % p) > 0))
}
