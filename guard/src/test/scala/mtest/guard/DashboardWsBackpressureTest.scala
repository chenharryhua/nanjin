package mtest.guard

import cats.effect.IO
import cats.effect.std.Queue
import fs2.Stream
import fs2.concurrent.Topic
import munit.CatsEffectSuite

import scala.concurrent.duration.DurationInt

/** Verification for the dashboard WebSocket fan-out fix (report item H1 / M2).
  *
  * These tests reproduce the exact structure `HttpWsRouter`'s `/ws` route uses to feed clients — one shared
  * `Topic`, one publisher, and per-client `circularBuffer` drainers — and assert the isolation property the
  * fix provides: a stalled client (one that never reads its buffer) can neither block the shared publisher
  * nor stall other clients, because each subscriber drains its `subscribeAwait` queue into a drop-oldest
  * buffer regardless of socket speed.
  *
  * The router's real `send` stream needs a `WebSocketBuilder2` (a running server), which makes a true
  * socket-stall end-to-end test flaky; modelling the fan-out directly keeps the property deterministic.
  * Production source is not modified by these tests.
  */
class DashboardWsBackpressureTest extends CatsEffectSuite {

  // the subscribeAwait bound the router uses
  private val subscribeBound = 5
  // a per-client drop-oldest buffer, as HttpWsRouter sizes it from maxPoints
  private val bufferCapacity = 8

  /** Mirrors `HttpWsRouter`: subscribe, then drain `live` into a circularBuffer via `offer` (never blocks,
    * drops oldest when full). Returns the per-client buffer so the test can choose to drain it or not.
    */
  private def clientBuffer(topic: Topic[IO, Long]): Stream[IO, Queue[IO, Long]] =
    Stream.resource(topic.subscribeAwait(subscribeBound)).flatMap { live =>
      Stream.eval(Queue.circularBuffer[IO, Long](bufferCapacity)).flatMap { buffer =>
        val drain: Stream[IO, Nothing] = live.foreach(buffer.offer).drain
        Stream.emit(buffer).concurrently(drain)
      }
    }

  test("1.a stalled client does not block the shared publisher") {
    // One subscriber that never drains its buffer. The publisher sends far more than subscribeBound +
    // bufferCapacity items. With the drop-oldest drainer the publisher must still complete; without it
    // (direct publish1 to a size-5 queue), publish1 would block forever once the queue filled.
    val program = for {
      topic <- Topic[IO, Long]
      // hold a stalled subscriber open for the duration of the publishing
      done <- clientBuffer(topic).flatMap { _ /* never read */ =>
        Stream.eval {
          // give the subscriber a moment to register, then publish a burst
          IO.sleep(100.millis) >>
            Stream.range(0, 1000).map(_.toLong).covary[IO].evalMap(topic.publish1).compile.drain.as(true)
        }
      }.compile.lastOrError.timeout(10.seconds)
    } yield done

    program.map(assert(_))
  }

  test("2.a stalled client does not stall a healthy client") {
    // Two subscribers: A never reads, B reads. B must still receive recent items while A is stalled.
    val program = Topic[IO, Long].flatMap { topic =>
      clientBuffer(topic).flatMap { _ => // A: never drained
        clientBuffer(topic).flatMap { healthy => // B: drained below
          val publish =
            Stream.sleep[IO](100.millis) ++
              Stream.range(0, 50).map(_.toLong).covary[IO].evalMap(topic.publish1).drain
          // B reads bufferCapacity items; it must get them despite A being stalled
          val readB = Stream.fromQueueUnterminated(healthy).take(bufferCapacity.toLong)
          readB.concurrently(publish)
        }
      }.compile.toList.timeout(10.seconds)
    }

    program.map { received =>
      // B received a full buffer's worth of items (it was never starved by A's stall)
      assertEquals(received.size, bufferCapacity)
      // and they are monotonically increasing Longs from the published 0..49 range
      assert(received == received.sorted)
      assert(received.forall(v => v >= 0L && v < 50L))
    }
  }

  test("3.a fast client receives items in published order") {
    // With a reader keeping up, no drops occur and order is preserved.
    val program = Topic[IO, Long].flatMap { topic =>
      clientBuffer(topic).flatMap { buffer =>
        val publish =
          Stream.sleep[IO](100.millis) ++
            Stream.range(0, bufferCapacity).map(_.toLong).covary[IO].evalMap(topic.publish1).drain
        Stream.fromQueueUnterminated(buffer).take(bufferCapacity.toLong).concurrently(publish)
      }.compile.toList.timeout(10.seconds)
    }

    program.map { received =>
      assertEquals(received, (0 until bufferCapacity).map(_.toLong).toList)
    }
  }
}
