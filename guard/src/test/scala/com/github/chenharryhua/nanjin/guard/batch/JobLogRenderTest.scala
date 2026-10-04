package com.github.chenharryhua.nanjin.guard.batch

import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.config.{Domain, Service, Task}
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope
import io.circe.Json
import munit.FunSuite
import org.typelevel.otel4s.trace.{SpanContext, TraceFlags, TraceState}
import scodec.bits.ByteVector

import scala.concurrent.duration.DurationInt

/** Lives in package `com.github.chenharryhua.nanjin.guard.batch` (not `mtest`) so it can reach the
  * package-private `JobLog` and `toLogEntry`. This lets the render matrix and privacy invariant be tested
  * directly and purely, without going through the effectful event pipeline.
  *
  * The auto-emitted per-job log (`JobLog.standalone`) never exposes produced values. Batch-nested entries
  * (`JobLog.inBatch`) are included only when a returned result is explicitly serialized; they include
  * produced values under the outcome tag. `MonadicBatch` can carry explicitly rendered per-job JSON and
  * serializes its successful final `A` as well.
  */
class JobLogRenderTest extends FunSuite {

  private val batchId: BatchId = BatchId(1L)
  private val scope =
    MetricScope(MetricScope.Label("batch"), Domain("test"), Service("test-service"), Task("task"))

  private def job(name: String, index: Int, mode: BatchMode, kind: Option[BatchKind]): Job =
    Job(name, index, scope, mode, kind, batchId)

  private def record(j: Job): JobRecord =
    JobRecord(j, 0.millis, 12.millis, None)

  // a valid span context with a known trace/span id, so the derived traceparent is deterministic
  private val traceIdHex = "0af7651916cd43dd8448eb211c80319c"
  private val spanIdHex = "b7ad6b7169203331"
  private val spanContext: SpanContext =
    SpanContext(
      traceId = ByteVector.fromValidHex(traceIdHex),
      spanId = ByteVector.fromValidHex(spanIdHex),
      traceFlags = TraceFlags.Sampled,
      traceState = TraceState.empty,
      remote = false
    )
  // the W3C traceparent for the above context: 00-<traceId>-<spanId>-01 (sampled)
  private val expectedTraceparent = s"00-$traceIdHex-$spanIdHex-01"

  private def tracedRecord(j: Job): JobRecord =
    JobRecord(j, 0.millis, 12.millis, Some(spanContext))

  private val quasiJob = job("work", 1, BatchMode.Sequential, Some(BatchKind.Quasi))
  private val valueJob = job("work", 1, BatchMode.Sequential, Some(BatchKind.Value))
  private val monadicJob = job("work", 1, BatchMode.Monadic, None)

  // a value that must never appear in any render produced by JobLog (it is the "user data")
  private val secret: Json = Json.fromString("TOP-SECRET-PRODUCED-VALUE")

  // ---- toLogEntry classification -------------------------------------------------------------------

  test("1.toLogEntry: a produced value that satisfied its predicate is Succeeded at Good level") {
    val entry = toLogEntry(JobState(record(quasiJob), JobFlag.Accepted, Right(secret)))
    assert(entry.message.isInstanceOf[JobLog.Succeeded[?]])
    assert(entry.level == LogLevel.Good)
    assert(entry.cause.isEmpty)
  }

  test("2.toLogEntry: a produced value rejected by its predicate is Unsatisfied at Warn level") {
    val entry = toLogEntry(JobState(record(quasiJob), JobFlag.Unmet, Right(secret)))
    assert(entry.message.isInstanceOf[JobLog.Unsatisfied[?]])
    assert(entry.level == LogLevel.Warn)
    assert(entry.cause.isEmpty)
  }

  test("3.toLogEntry: a Left flagged Accepted or Unmet is Nonfatal at Warn level (failure is retained)") {
    val ex = new RuntimeException("boom")
    List(JobFlag.Accepted, JobFlag.Unmet).foreach { flag =>
      val entry = toLogEntry(JobState[Json](record(quasiJob), flag, Left(ex)))
      assert(entry.message.isInstanceOf[JobLog.Nonfatal[?]])
      assert(entry.level == LogLevel.Warn)
      assert(entry.cause.contains(ex))
    }
  }

  test("4.toLogEntry: an exception in a Value job is Critical at Error level (fatal to the batch)") {
    val ex = new RuntimeException("boom")
    val entry = toLogEntry(JobState[Json](record(valueJob), JobFlag.Failed, Left(ex)))
    assert(entry.message.isInstanceOf[JobLog.Critical[?]])
    assert(entry.level == LogLevel.Error)
    assert(entry.cause.contains(ex))
  }

  test("5.toLogEntry: an exception in a monadic job (kind = None) is Critical at Error level") {
    val ex = new RuntimeException("boom")
    val entry = toLogEntry(JobState[Json](record(monadicJob), JobFlag.Failed, Left(ex)))
    assert(entry.message.isInstanceOf[JobLog.Critical[?]])
    assert(entry.level == LogLevel.Error)
    assert(entry.cause.contains(ex))
  }

  // ---- standalone render (the AUTO-EMITTED log) ----------------------------------------------------

  test("6.standalone Succeeded: identity + took only, and the produced value is dropped even when present") {
    // the value is carried on the case, but standalone deliberately discards it (privacy)
    val js = JobLog.Succeeded(record(quasiJob), secret).standalone
    val c = js.hcursor
    // the status tag holds the full job object; identity and context live under it.
    // Assert the literal wire key (not the derived tag) so this guards against drift in the derivation.
    val tag = c.downField("succeeded")
    assert(tag.get[String]("job-1").toOption.contains("work")) // identity
    assert(tag.get[String]("Sequential Quasi Batch").toOption.contains("batch")) // full job context
    assert(c.get[String]("took").toOption.exists(_.nonEmpty)) // took, its own key
    assert(c.downField("result").focus.isEmpty) // privacy: value dropped
    assert(!js.noSpaces.contains("TOP-SECRET")) // the produced value never reaches the auto-emitted log
  }

  test("7.standalone Unsatisfied: status tag holds the job, took its own key, produced value dropped") {
    val js = JobLog.Unsatisfied(record(quasiJob), secret).standalone
    val c = js.hcursor
    assert(c.downField("unsatisfied").get[String]("job-1").toOption.contains("work"))
    assert(c.get[String]("took").toOption.exists(_.nonEmpty))
    assert(c.downField("result").focus.isEmpty)
    assert(!js.noSpaces.contains("TOP-SECRET"))
  }

  test("8.standalone Nonfatal: job under the status tag, took present, no duplicated error message") {
    val js = JobLog.Nonfatal(record(quasiJob), new RuntimeException("boom")).standalone
    val c = js.hcursor
    // stable: the failure is tagged, carries the job identity, and reports took
    assert(c.downField("nonfatal").get[String]("job-1").toOption.contains("work"))
    assert(c.get[String]("took").toOption.exists(_.nonEmpty))
    // standalone deliberately omits the error message: it is auto-emitted with the throwable as the log
    // entry's cause, so the stacktrace already carries it and repeating it here would duplicate.
    assert(c.downField("error").focus.isEmpty)
    assert(c.downField("result").focus.isEmpty)
  }

  test("9.standalone Critical: job under the status tag, took present, no duplicated error message") {
    val js = JobLog.Critical(record(valueJob), new RuntimeException("boom")).standalone
    val c = js.hcursor
    assert(c.downField("critical").get[String]("job-1").toOption.contains("work"))
    assert(c.get[String]("took").toOption.exists(_.nonEmpty))
    assert(c.downField("error").focus.isEmpty)
    assert(c.downField("result").focus.isEmpty)
  }

  test("10.standalone Kickoff/Canceled: render the job under their lifecycle key") {
    val kickoff = JobLog.Kickoff(quasiJob).standalone
    val canceled = JobLog.Canceled(quasiJob).standalone
    assert(kickoff.hcursor.downField("kickoff").get[String]("job-1").toOption.contains("work"))
    assert(canceled.hcursor.downField("canceled").get[String]("job-1").toOption.contains("work"))
  }

  // ---- inBatch render (nested inside a serialized BatchResult) -------------------------------------

  // These `inBatch` tests assert stable display invariants (identity, status tag, took, and payload
  // disclosure) rather than the exact key layout, which is UI-facing and expected to evolve.

  test("11.inBatch Succeeded: identity, status tag, took present, and the produced value is disclosed") {
    val text = JobLog.Succeeded(record(quasiJob), secret).inBatch.noSpaces
    assert(text.contains("job-1") && text.contains("work")) // job identity
    assert(text.contains("succeeded")) // status tag
    assert(text.contains("12 milli")) // took (record uses 12.millis)
    // inBatch is the user-triggered serialization path, so the produced value is shown here
    assert(text.contains("TOP-SECRET-PRODUCED-VALUE"))
  }

  test("12.inBatch Succeeded with Json.Null: null remains under the outcome tag") {
    val js = JobLog.Succeeded(record(quasiJob), Json.Null).inBatch
    val text = js.noSpaces
    assert(text.contains("job-1") && text.contains("work"))
    assert(text.contains("succeeded"))
    // Null remains as the outcome's payload under its status tag.
    assertEquals(js.hcursor.downField("succeeded").focus, Some(Json.Null))
  }

  test("13.inBatch Critical: identity, status tag, took, and the exception message are rendered") {
    // Critical carries no produced value, so its phantom `A` is pinned to Unit for the Encoder to resolve
    val text =
      JobLog.Critical[Unit](record(monadicJob), new RuntimeException("boom")).inBatch.noSpaces
    assert(text.contains("job-1") && text.contains("work"))
    assert(text.contains("critical"))
    assert(text.contains("boom")) // exception message surfaces on the batch-nested render
  }

  // Note: Kickoff/Canceled extend JobLog[Nothing], so `inBatch` (which needs an Encoder[A]) is uncallable
  // for them by construction — matching the "should not happen" comment on those arms. Their real render is
  // `standalone`, covered above.

  // ---- traceparent (traced batches) ----------------------------------------------------------------

  // A traced job carries a span context; both renders expose it as a W3C `traceparent` key. Untraced jobs
  // (spanContext = None) add no such key. Covers all four outcome cases across standalone and inBatch.

  test("14.standalone: a traced job renders traceparent for every outcome case") {
    val succeeded = JobLog.Succeeded(tracedRecord(quasiJob), secret).standalone
    val unsatisfied = JobLog.Unsatisfied(tracedRecord(quasiJob), secret).standalone
    val nonfatal = JobLog.Nonfatal(tracedRecord(quasiJob), new RuntimeException("boom")).standalone
    val critical = JobLog.Critical(tracedRecord(valueJob), new RuntimeException("boom")).standalone
    List(succeeded, unsatisfied, nonfatal, critical).foreach { js =>
      assertEquals(js.hcursor.get[String]("traceparent").toOption, Some(expectedTraceparent))
    }
  }

  test("15.inBatch: a traced job renders traceparent for every outcome case") {
    val succeeded = JobLog.Succeeded(tracedRecord(quasiJob), secret).inBatch
    val unsatisfied = JobLog.Unsatisfied(tracedRecord(quasiJob), secret).inBatch
    val nonfatal = JobLog.Nonfatal[Json](tracedRecord(quasiJob), new RuntimeException("boom")).inBatch
    val critical = JobLog.Critical[Json](tracedRecord(valueJob), new RuntimeException("boom")).inBatch
    List(succeeded, unsatisfied, nonfatal, critical).foreach { js =>
      assertEquals(js.hcursor.get[String]("traceparent").toOption, Some(expectedTraceparent))
    }
  }

  test("16.an untraced job (spanContext = None) renders no traceparent key") {
    val standalone = JobLog.Succeeded(record(quasiJob), secret).standalone
    val inBatch = JobLog.Succeeded(record(quasiJob), secret).inBatch
    assert(standalone.hcursor.downField("traceparent").focus.isEmpty)
    assert(inBatch.hcursor.downField("traceparent").focus.isEmpty)
  }
}
