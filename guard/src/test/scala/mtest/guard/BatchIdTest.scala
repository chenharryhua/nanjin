package mtest.guard

import cats.effect.{IO, Resource}
import cats.Order
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.BatchId
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.CatsEffectSuite

class BatchIdTest extends CatsEffectSuite {
  private val service: ServiceGuard[IO] = TaskGuard[IO]("batch-id").service("batch-id")


  test("1.apply then value round-trips the underlying Long") {
    assert(BatchId(1L).value == 1L)
    assert(BatchId(0L).value == 0L)
    assert(BatchId(-1L).value == -1L)
    assert(BatchId(Long.MaxValue).value == Long.MaxValue)
    assert(BatchId(Long.MinValue).value == Long.MinValue)
  }

  test("2.Show renders the plain number") {
    assert(BatchId(1L).show == "1")
    assert(BatchId(0L).show == "0")
    assert(BatchId(Long.MaxValue).show == Long.MaxValue.toString)
  }

  test("3.Order compares by the underlying Long") {
    assert(Order[BatchId].compare(BatchId(1L), BatchId(2L)) < 0)
    assert(Order[BatchId].compare(BatchId(2L), BatchId(1L)) > 0)
    assert(Order[BatchId].compare(BatchId(3L), BatchId(3L)) == 0)
  }

  test("4.Ordering sorts by the underlying Long") {
    val sorted = List(BatchId(3L), BatchId(1L), BatchId(2L)).sorted
    assert(sorted.map(_.value) == List(1L, 2L, 3L))
  }

  test("5.encodes as a bare JSON number") {
    assert(BatchId(42L).asJson == Json.fromLong(42L))
    // guards the wire form: the id must stay a number, never an object or string
    assert(BatchId(42L).asJson.isNumber)
  }

  test("6.codec round-trips: decode(encode(id)) == id") {
    val ids = List(0L, 1L, -7L, Long.MaxValue, Long.MinValue).map(BatchId(_))
    ids.foreach { id =>
      val decoded = id.asJson.as[BatchId]
      assert(decoded == Right(id))
    }
  }

  test("7.Decoder reads a plain JSON number") {
    assert(Json.fromLong(99L).as[BatchId] == Right(BatchId(99L)))
  }

  test("8.batch IDs are allocated for each effect execution") {
    def repeated(effect: IO[Long]): IO[(Long, Long)] =
      for {
        first <- effect
        second <- effect
      } yield first -> second

    def repeatedResource[A](resource: Resource[IO, A], id: A => Long): IO[(Long, Long)] =
      repeated(resource.use(value => IO.pure(id(value))))

    service.eventStream { agent =>
      val batch = agent.batch("batch-id").sequential("job" -> IO.pure(1))
      val batchLight = agent.batchLight("batch-light-id").sequential("job" -> IO.pure(1))
      val batchTraced = agent.batchTraced("batch-traced-id", _.build).sequential("job" -> (_ => IO.pure(1)))

      for {
        batchQuasi <- repeatedResource(batch.quasiBatch, _.batchId.value)
        batchValue <- repeatedResource(batch.valueBatch, _.batchId.value)
        batchMonadic <- repeatedResource(
          agent.batch("batch-monadic-id").monadic(job => job("job", IO.pure(1))).monadicBatch,
          _.batchId.value)
        lightQuasi <- repeated(batchLight.quasiBatch.map(_.batchId.value))
        lightValue <- repeated(batchLight.valueBatch.map(_.batchId.value))
        lightMonadic <- repeated(
          agent.batchLight("light-monadic-id").monadic(job => job("job", IO.pure(1))).monadicBatch.map(_.batchId.value))
        tracedQuasi <- repeated(batchTraced.quasiBatch.map(_.batchId.value))
        tracedValue <- repeated(batchTraced.valueBatch.map(_.batchId.value))
        tracedMonadic <- repeated(
          agent
            .batchTraced("traced-monadic-id", _.build)
            .monadic(job => job("job", IO.pure(1)))
            .monadicBatch
            .map(_.batchId.value))
        _ <- IO {
          List(
            batchQuasi,
            batchValue,
            batchMonadic,
            lightQuasi,
            lightValue,
            lightMonadic,
            tracedQuasi,
            tracedValue,
            tracedMonadic
          ).foreach { case (first, second) => assert(first != second) }
        }
      } yield ()
    }.compile.lastOrError.map { event =>
      assertEquals(event.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }
}
