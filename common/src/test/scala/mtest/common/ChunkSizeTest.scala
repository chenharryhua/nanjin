package mtest.common

import cats.implicits.toShow
import cats.syntax.order.catsSyntaxPartialOrder
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.common.ChunkSize
import io.circe.syntax.given

class ChunkSizeTest extends AnyFunSuite {
  def fun(cs: ChunkSize): Unit = println(cs.show)

  test("1.chunk size - function") {
    fun(ChunkSize(10))
    fun(100)
  }

  test("2.chunk size - assignment") {
    val cs = ChunkSize(10)
    val cs2: ChunkSize = 100
    println((cs, cs2))
  }

  test("3.chunk size - json") {
    val cs = ChunkSize(10)
    val cs2: ChunkSize = -100
    println((cs.asJson, cs2.asJson))
  }

  test("4.order") {
    assert(ChunkSize(3) > ChunkSize(2))
  }

}
