package mtest.guard

import cats.Order
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.batch.BatchId
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

class BatchIdTest extends AnyFunSuite {

  test("apply then value round-trips the underlying Long") {
    assert(BatchId(1L).value == 1L)
    assert(BatchId(0L).value == 0L)
    assert(BatchId(-1L).value == -1L)
    assert(BatchId(Long.MaxValue).value == Long.MaxValue)
    assert(BatchId(Long.MinValue).value == Long.MinValue)
  }

  test("Show renders the plain number") {
    assert(BatchId(1L).show == "1")
    assert(BatchId(0L).show == "0")
    assert(BatchId(Long.MaxValue).show == Long.MaxValue.toString)
  }

  test("Order compares by the underlying Long") {
    assert(Order[BatchId].compare(BatchId(1L), BatchId(2L)) < 0)
    assert(Order[BatchId].compare(BatchId(2L), BatchId(1L)) > 0)
    assert(Order[BatchId].compare(BatchId(3L), BatchId(3L)) == 0)
  }

  test("Ordering sorts by the underlying Long") {
    val sorted = List(BatchId(3L), BatchId(1L), BatchId(2L)).sorted
    assert(sorted.map(_.value) == List(1L, 2L, 3L))
  }

  test("encodes as a bare JSON number") {
    assert(BatchId(42L).asJson == Json.fromLong(42L))
    // guards the wire form: batch_id must stay a number, never an object or string
    assert(BatchId(42L).asJson.isNumber)
  }

  test("codec round-trips: decode(encode(id)) == id") {
    val ids = List(0L, 1L, -7L, Long.MaxValue, Long.MinValue).map(BatchId(_))
    ids.foreach { id =>
      val decoded = id.asJson.as[BatchId]
      assert(decoded == Right(id))
    }
  }

  test("Decoder reads a plain JSON number") {
    assert(Json.fromLong(99L).as[BatchId] == Right(BatchId(99L)))
  }
}
