package com.github.chenharryhua.nanjin.guard.config

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.TaskGuard
import org.scalatest.funsuite.AnyFunSuite

class DigestedNameTest extends AnyFunSuite {
  val s = TaskGuard[IO]("digest").service("abc")
  test("somehow ordered") {
    val d1               = DigestedName(List("a", "b", "c"), s.serviceParams)
    val d2               = DigestedName(List("a", "b"), s.serviceParams)
    val d3               = DigestedName(List("a"), s.serviceParams)
    val d4               = DigestedName(List("a", "c"), s.serviceParams)
    val List(a, b, c, d) = List(d2.metricRepr, d4.metricRepr, d3.metricRepr, d1.metricRepr).sorted
    println(a, b, c, d)
    assert(a === "[a/b/c][16e09482]")
    assert(b === "[a/b][ade83c5a]")
    assert(c === "[a/c][52cdec0f]")
    assert(d === "[a][8622b671]")
  }
}
