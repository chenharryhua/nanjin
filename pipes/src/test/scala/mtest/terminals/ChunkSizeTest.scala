package com.github.chenharryhua.nanjin.terminals

import org.scalatest.funsuite.AnyFunSuite

class ChunkSizeTest extends AnyFunSuite {
  def fun(cs: ChunkSize): Unit = assert(cs.value > 0): Unit

  test("1.chunk size - construct and value") {
    fun(ChunkSize(10))
    fun(ChunkSize(100))
    assert(ChunkSize(10).value == 10)
  }

  test("2.chunk size - validation") {
    assert(intercept[IllegalArgumentException](ChunkSize(0)).getMessage.contains("but was 0"))
    assert(intercept[IllegalArgumentException](ChunkSize(-100)).getMessage.contains("but was -100"))
  }
}
