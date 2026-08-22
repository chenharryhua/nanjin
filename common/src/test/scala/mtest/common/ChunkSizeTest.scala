package mtest.common

import cats.syntax.order.catsSyntaxPartialOrder
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.common.ChunkSize
import io.circe.jawn.decode
import io.circe.syntax.given

class ChunkSizeTest extends AnyFunSuite {
  def fun(cs: ChunkSize): Unit = assert(cs.value > 0): Unit

  test("1.chunk size - function") {
    fun(ChunkSize(10))
    fun(100)
  }

  test("2.chunk size - assignment") {
    val cs = ChunkSize(10)
    val cs2: ChunkSize = 100
    assert(cs.value > 0 && cs2.value > 0)
  }

  test("3.chunk size - json and validation") {
    val cs = ChunkSize(10)
    assert(cs.asJson.toString == "10")
    assert(decode[ChunkSize]("10").map(_.value) == Right(10))
    assert(decode[ChunkSize]("0").isLeft)
    assert(decode[ChunkSize]("-100").isLeft)
    assert(intercept[IllegalArgumentException](ChunkSize(0)).getMessage.contains("but was 0"))
    assert(intercept[IllegalArgumentException](ChunkSize(-100)).getMessage.contains("but was -100"))
    assert(intercept[IllegalArgumentException] {
      val invalid: ChunkSize = -100
      invalid
    }.getMessage.contains("but was -100"))
  }

  test("4.order") {
    assert(ChunkSize(3) > ChunkSize(2))
  }

}
