package com.github.chenharryhua.nanjin.guard.batch

import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.config.{Domain, Service, Task}
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope
import io.circe.Json
import munit.FunSuite

import scala.concurrent.duration.DurationInt

/** Lives in package `com.github.chenharryhua.nanjin.guard.batch` (not `mtest`) so it can reach the
  * package-private `JobLog` and `toLogEntry`. This lets the render matrix and the privacy invariant be tested
  * directly and purely, without going through the effectful event pipeline.
  *
  * The central property under test: the auto-emitted per-job log (`JobLog.standalone`) and the batch-nested
  * per-job entry (`JobLog.inBatch`) render only lifecycle facts — identity, took, outcome tag, and (on
  * failure) the exception message — never the job's produced value. The produced value is added only by the
  * `QuasiBatch`/`ValueBatch` encoders, which run solely when the user chooses to serialize a returned result.
  * The `MonadicBatch` encoder renders no produced value at all (its jobs are `JobState[Unit]` and its final
  * `A` is not serialized).
  */
class JobLogRenderTest extends FunSuite {

  private val batchId: BatchId = BatchId(1L)
  private val scope =
    MetricScope(MetricScope.Label("batch"), Domain("test"), Service("test-service"), Task("task"))

  private def job(name: String, index: Int, mode: BatchMode, kind: Option[BatchKind]): Job =
    Job(name, index, scope, mode, kind, batchId)

  private def record(j: Job, succeeded: Boolean): JobRecord =
    JobRecord(j, 0.millis, 12.millis, succeeded = succeeded)

  private val quasiJob = job("work", 1, BatchMode.Sequential, Some(BatchKind.Quasi))
  private val valueJob = job("work", 1, BatchMode.Sequential, Some(BatchKind.Value))
  private val monadicJob = job("work", 1, BatchMode.Monadic, None)

  // a value that must never appear in any render produced by JobLog (it is the "user data")
  private val secret: Json = Json.fromString("TOP-SECRET-PRODUCED-VALUE")

  // ---- toLogEntry classification -------------------------------------------------------------------

  test("1.toLogEntry: a produced value that satisfied its predicate is Succeeded at Good level") {
    val entry = toLogEntry(JobState(record(quasiJob, succeeded = true), Right(secret)))
    assert(entry.message.isInstanceOf[JobLog.Succeeded[?]])
    assert(entry.level == LogLevel.Good)
    assert(entry.cause.isEmpty)
  }

  test("2.toLogEntry: a produced value rejected by its predicate is Unsatisfied at Warn level") {
    val entry = toLogEntry(JobState(record(quasiJob, succeeded = false), Right(secret)))
    assert(entry.message.isInstanceOf[JobLog.Unsatisfied[?]])
    assert(entry.level == LogLevel.Warn)
    assert(entry.cause.isEmpty)
  }

  test("3.toLogEntry: an exception in a Quasi job is Nonfatal at Warn level (failure is retained)") {
    val ex = new RuntimeException("boom")
    val entry = toLogEntry(JobState[Json](record(quasiJob, succeeded = false), Left(ex)))
    assert(entry.message.isInstanceOf[JobLog.Nonfatal[?]])
    assert(entry.level == LogLevel.Warn)
    assert(entry.cause.contains(ex))
  }

  test("4.toLogEntry: an exception in a Value job is Critical at Error level (fatal to the batch)") {
    val ex = new RuntimeException("boom")
    val entry = toLogEntry(JobState[Json](record(valueJob, succeeded = false), Left(ex)))
    assert(entry.message.isInstanceOf[JobLog.Critical[?]])
    assert(entry.level == LogLevel.Error)
    assert(entry.cause.contains(ex))
  }

  test("5.toLogEntry: an exception in a monadic job (kind = None) is Critical at Error level") {
    val ex = new RuntimeException("boom")
    val entry = toLogEntry(JobState[Json](record(monadicJob, succeeded = false), Left(ex)))
    assert(entry.message.isInstanceOf[JobLog.Critical[?]])
    assert(entry.level == LogLevel.Error)
    assert(entry.cause.contains(ex))
  }

  // ---- standalone render (the AUTO-EMITTED log) ----------------------------------------------------

  test("6.standalone Succeeded: identity + took only, and the produced value is dropped even when present") {
    // the value is carried on the case, but standalone deliberately discards it (privacy)
    val js = JobLog.Succeeded(record(quasiJob, succeeded = true), secret).standalone
    val c = js.hcursor
    // the status tag holds the full job object; identity and context live under it.
    // Assert the literal wire key (not the derived tag) so this guards against drift in the derivation.
    val tag = c.downField("succeeded")
    assert(tag.get[String]("job-1").toOption.contains("work")) // identity
    assert(tag.get[String]("Sequential Quasi Batch").toOption.contains("batch")) // full job context
    assert(c.get[String](JobLog.TOOK).toOption.exists(_.nonEmpty)) // took, its own key
    assert(c.downField(JobLog.RESULT).focus.isEmpty) // privacy: value dropped
    assert(!js.noSpaces.contains("TOP-SECRET")) // the produced value never reaches the auto-emitted log
  }

  test("7.standalone Unsatisfied: status tag holds the job, took its own key, produced value dropped") {
    val js = JobLog.Unsatisfied(record(quasiJob, succeeded = false), secret).standalone
    val c = js.hcursor
    assert(c.downField("unsatisfied").get[String]("job-1").toOption.contains("work"))
    assert(c.get[String](JobLog.TOOK).toOption.exists(_.nonEmpty))
    assert(c.downField(JobLog.RESULT).focus.isEmpty)
    assert(!js.noSpaces.contains("TOP-SECRET"))
  }

  test("8.standalone Nonfatal: job under the status tag, took present, no duplicated error message") {
    val js = JobLog.Nonfatal(record(quasiJob, succeeded = false), new RuntimeException("boom")).standalone
    val c = js.hcursor
    // stable: the failure is tagged, carries the job identity, and reports took
    assert(c.downField("nonfatal").get[String]("job-1").toOption.contains("work"))
    assert(c.get[String](JobLog.TOOK).toOption.exists(_.nonEmpty))
    // standalone deliberately omits the error message: it is auto-emitted with the throwable as the log
    // entry's cause, so the stacktrace already carries it and repeating it here would duplicate.
    assert(c.downField(JobLog.ERROR).focus.isEmpty)
    assert(c.downField(JobLog.RESULT).focus.isEmpty)
  }

  test("9.standalone Critical: job under the status tag, took present, no duplicated error message") {
    val js = JobLog.Critical(record(valueJob, succeeded = false), new RuntimeException("boom")).standalone
    val c = js.hcursor
    assert(c.downField("critical").get[String]("job-1").toOption.contains("work"))
    assert(c.get[String](JobLog.TOOK).toOption.exists(_.nonEmpty))
    assert(c.downField(JobLog.ERROR).focus.isEmpty)
    assert(c.downField(JobLog.RESULT).focus.isEmpty)
  }

  test("10.standalone Kickoff/Canceled: render the job under their lifecycle key") {
    val kickoff = JobLog.Kickoff(quasiJob).standalone
    val canceled = JobLog.Canceled(quasiJob).standalone
    assert(kickoff.hcursor.downField("kickoff").get[String]("job-1").toOption.contains("work"))
    assert(canceled.hcursor.downField("canceled").get[String]("job-1").toOption.contains("work"))
  }

  // ---- inBatch render (nested inside a serialized BatchResult) -------------------------------------

  // These `inBatch` tests assert stable display invariants (identity, status tag, took, and value/error
  // disclosure) rather than the exact key layout, which is UI-facing and expected to evolve.

  test("11.inBatch Succeeded: identity, status tag, took present, and the produced value is disclosed") {
    val text = JobLog.Succeeded(record(quasiJob, succeeded = true), secret).inBatch.noSpaces
    assert(text.contains("job-1") && text.contains("work")) // job identity
    assert(text.contains("succeeded")) // status tag
    assert(text.contains("12 milli")) // took (record uses 12.millis)
    // inBatch is the user-triggered serialization path, so the produced value is shown here
    assert(text.contains("TOP-SECRET-PRODUCED-VALUE"))
  }

  test("12.inBatch Succeeded with an absent value (Json.Null): no result is rendered") {
    val js = JobLog.Succeeded(record(quasiJob, succeeded = true), Json.Null).inBatch
    val text = js.noSpaces
    assert(text.contains("job-1") && text.contains("work"))
    assert(text.contains("succeeded"))
    // dropNullValues removes the absent value: no result key survives
    assert(js.hcursor.downField(JobLog.RESULT).focus.isEmpty)
  }

  test("13.inBatch Critical: identity, status tag, took, and the exception message are rendered") {
    // Critical carries no produced value, so its phantom `A` is pinned to Unit for the Encoder to resolve
    val text =
      JobLog.Critical[Unit](
        record(monadicJob, succeeded = false),
        new RuntimeException("boom")).inBatch.noSpaces
    assert(text.contains("job-1") && text.contains("work"))
    assert(text.contains("critical"))
    assert(text.contains("boom")) // exception message surfaces on the batch-nested render
  }

  // Note: Kickoff/Canceled extend JobLog[Nothing], so `inBatch` (which needs an Encoder[A]) is uncallable
  // for them by construction — matching the "should not happen" comment on those arms. Their real render is
  // `standalone`, covered above.
}
