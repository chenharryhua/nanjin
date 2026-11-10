package mtest.guard

import cats.effect.IO
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.opentelemetry.api.common.{AttributeKey, Attributes as JAttributes}
import io.opentelemetry.api.trace.StatusCode as JStatusCode
import io.opentelemetry.sdk.trace.data.SpanData
import munit.CatsEffectSuite
import org.typelevel.otel4s.oteljava.testkit.trace.TracesTestkit
import org.typelevel.otel4s.trace.Span

import scala.jdk.CollectionConverters.given

/** Verifies the OpenTelemetry arm of `agent.batchTraced`: what the exported spans actually say.
  *
  * The other traced-batch suite (`BatchTracedTest`) runs against `TracerProvider.noop` and so can only assert
  * on the returned `BatchResult`. Here the oteljava in-memory testkit is supplied as the service's tracer
  * provider, so the spans themselves can be inspected.
  *
  * Two properties are pinned:
  *   - a job that fails records its exception on '''its own''' span and sets that span's status to `ERROR`.
  *     This cannot be left to otel4s: a job's `attempt` runs inside its span's scope so a quasi batch can
  *     retain the failure, which means the span's `use` always succeeds. A quasi batch does not re-raise
  *     either, so without the explicit recording a batch whose every job threw would export a trace
  *     indistinguishable from one where all succeeded.
  *   - a job span carries the batch's scope attributes plus the batch id and the job's index. OpenTelemetry
  *     does not propagate a parent's attributes to its children, so these have to be stamped on each child.
  *
  * Production source is not modified.
  */
class OtelTracedBatchTest extends CatsEffectSuite {

  private val boom: Exception = new Exception("job blew up")

  private def service(testkit: TracesTestkit[IO]): ServiceGuard[IO] =
    TaskGuard[IO]("traced")
      .service("batch")
      .updateConfig(_.withTracerProvider(Resource.pure(testkit.tracerProvider)))

  private def named(spans: List[SpanData], name: String): SpanData =
    spans.find(_.getName == name).getOrElse(fail(s"no span named '$name' in ${spans.map(_.getName)}"))

  private def stringAttr(attributes: JAttributes, key: String): Option[String] =
    Option(attributes.get(AttributeKey.stringKey(key)))

  private def longAttr(attributes: JAttributes, key: String): Option[Long] =
    Option(attributes.get(AttributeKey.longKey(key))).map(_.longValue)

  test("1.a failed job in a quasi batch is recorded on its own span") {
    TracesTestkit.inMemory[IO]().use { testkit =>
      service(testkit).eventStream { agent =>
        agent
          .batchTraced("payments", _.build)
          .sequential(
            "good" -> ((_: Span[IO]) => IO(1)),
            "bad" -> ((_: Span[IO]) => IO.raiseError[Int](boom)))
          .quasiBatch
          .map { qb =>
            assertEquals(qb.outcomes.size, 2)
            assert(!qb.allSucceeded)
          }
      }.compile.drain >> testkit.finishedSpans
    }.map { spans =>
      val bad = named(spans, "bad")
      val good = named(spans, "good")
      val parent = named(spans, "payments")

      // the failing job's span carries the error
      assertEquals(bad.getStatus.getStatusCode, JStatusCode.ERROR)
      assert(bad.getStatus.getDescription.contains("job blew up"))
      assertEquals(bad.getEvents.asScala.toList.map(_.getName), List("exception"))

      // the succeeding job's span is untouched
      assertEquals(good.getStatus.getStatusCode, JStatusCode.UNSET)
      assert(good.getEvents.asScala.isEmpty)

      // the parent span is NOT errored: a quasi batch retains failures instead of raising, so nothing
      // propagates past the job. That is why recording on the child span is what makes the failure visible.
      assertEquals(parent.getStatus.getStatusCode, JStatusCode.UNSET)

      // each job span nests under the batch's parent span
      assertEquals(bad.getParentSpanId, parent.getSpanId)
      assertEquals(good.getParentSpanId, parent.getSpanId)
    }
  }

  test("2.job spans carry the batch scope, batch id and job index") {
    TracesTestkit.inMemory[IO]().use { testkit =>
      service(testkit).eventStream { agent =>
        agent
          .batchTraced("payments", _.build)
          .sequential("good" -> ((_: Span[IO]) => IO(1)), "also" -> ((_: Span[IO]) => IO(2)))
          .valueBatch
          .void
      }.compile.drain >> testkit.finishedSpans
    }.map { spans =>
      val good = named(spans, "good")
      val also = named(spans, "also")
      val attributes = good.getAttributes

      assertEquals(stringAttr(attributes, "nj.label"), Some("payments"))
      assertEquals(stringAttr(attributes, "nj.domain"), Some("main"))
      assertEquals(stringAttr(attributes, "nj.service"), Some("batch"))
      assertEquals(stringAttr(attributes, "nj.task"), Some("traced"))
      assertEquals(longAttr(attributes, "nj.job.index"), Some(1L))
      assertEquals(longAttr(also.getAttributes, "nj.job.index"), Some(2L))

      // both jobs of one run share a batch id
      val batchId = longAttr(attributes, "nj.batch.id")
      assert(batchId.exists(_ > 0L))
      assertEquals(longAttr(also.getAttributes, "nj.batch.id"), batchId)

      // the parent span keeps the identity dimensions but not nj.label: its own name is the label
      val parent = named(spans, "payments").getAttributes
      assertEquals(stringAttr(parent, "nj.service"), Some("batch"))
      assertEquals(stringAttr(parent, "nj.label"), None)
    }
  }

  test("3.a failed monadic job is recorded on its own span") {
    TracesTestkit.inMemory[IO]().use { testkit =>
      service(testkit).eventStream { agent =>
        agent
          .batchTraced("chain", _.build)
          .monadic { builder =>
            for {
              a <- builder("first", IO(1))
              b <- builder("second", IO.raiseError[Int](boom))
            } yield a + b
          }
          .monadicBatch
          .map(mb => assert(mb.result.isLeft))
      }.compile.drain >> testkit.finishedSpans
    }.map { spans =>
      val second = named(spans, "second")
      assertEquals(second.getStatus.getStatusCode, JStatusCode.ERROR)
      assertEquals(second.getEvents.asScala.toList.map(_.getName), List("exception"))
      assertEquals(longAttr(second.getAttributes, "nj.job.index"), Some(2L))

      val first = named(spans, "first")
      assertEquals(first.getStatus.getStatusCode, JStatusCode.UNSET)
    }
  }

  test("4.a value batch errors the parent span as well as the job span") {
    TracesTestkit.inMemory[IO]().use { testkit =>
      service(testkit).eventStream { agent =>
        agent
          .batchTraced("payments", _.build)
          .sequential("bad" -> ((_: Span[IO]) => IO.raiseError[Int](boom)))
          .valueBatch
          .attempt
          .map(e => assert(e.isLeft))
      }.compile.drain >> testkit.finishedSpans
    }.map { spans =>
      // the job span is errored by the explicit recording, the parent by otel4s because `rethrow` runs
      // inside `parent.surround`
      assertEquals(named(spans, "bad").getStatus.getStatusCode, JStatusCode.ERROR)
      assertEquals(named(spans, "payments").getStatus.getStatusCode, JStatusCode.ERROR)
    }
  }
}
