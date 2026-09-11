package com.github.chenharryhua.nanjin.guard.batch

import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.config.{Domain, Service, Task}
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

/** Lives in package `com.github.chenharryhua.nanjin.guard.batch` (not `mtest`) so it can reach the
  * package-private `JobLog`, `toLogEntry`, and `JsonKeys`. This lets the render matrix and the privacy
  * invariant be tested directly and purely, without going through the effectful event pipeline.
  *
  * The central property under test: the auto-emitted per-job log (`JobLog.standalone`) and the batch-nested
  * per-job entry (`JobLog.inBatch`) render only lifecycle facts — identity, took, outcome tag, and (on
  * failure) the exception message — never the job's produced value. The produced value is added only by the
  * `QuasiBatch`/`ValueBatch`/`MonadicBatch` encoders, which run solely when the user chooses to serialize a
  * returned result.
  */
class JobLogRenderTest extends AnyFunSuite {

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

  test("toLogEntry: a produced value that satisfied its predicate is Succeeded at Good level") {
    val entry = toLogEntry(JobState(record(quasiJob, succeeded = true), Right(secret)))
    assert(entry.message.isInstanceOf[JobLog.Succeeded[?]])
    assert(entry.level == LogLevel.Good)
    assert(entry.cause.isEmpty)
  }

  test("toLogEntry: a produced value rejected by its predicate is Unsatisfied at Warn level") {
    val entry = toLogEntry(JobState(record(quasiJob, succeeded = false), Right(secret)))
    assert(entry.message.isInstanceOf[JobLog.Unsatisfied[?]])
    assert(entry.level == LogLevel.Warn)
    assert(entry.cause.isEmpty)
  }

  test("toLogEntry: an exception in a Quasi job is Nonfatal at Warn level (failure is retained)") {
    val ex = new RuntimeException("boom")
    val entry = toLogEntry(JobState[Json](record(quasiJob, succeeded = false), Left(ex)))
    assert(entry.message.isInstanceOf[JobLog.Nonfatal[?]])
    assert(entry.level == LogLevel.Warn)
    assert(entry.cause.contains(ex))
  }

  test("toLogEntry: an exception in a Value job is Critical at Error level (fatal to the batch)") {
    val ex = new RuntimeException("boom")
    val entry = toLogEntry(JobState[Json](record(valueJob, succeeded = false), Left(ex)))
    assert(entry.message.isInstanceOf[JobLog.Critical[?]])
    assert(entry.level == LogLevel.Error)
    assert(entry.cause.contains(ex))
  }

  test("toLogEntry: an exception in a monadic job (kind = None) is Critical at Error level") {
    val ex = new RuntimeException("boom")
    val entry = toLogEntry(JobState[Json](record(monadicJob, succeeded = false), Left(ex)))
    assert(entry.message.isInstanceOf[JobLog.Critical[?]])
    assert(entry.level == LogLevel.Error)
    assert(entry.cause.contains(ex))
  }

  // ---- standalone render (the AUTO-EMITTED log) ----------------------------------------------------

  test("standalone Succeeded: identity + took only, and the produced value is dropped even when present") {
    // the value is carried on the case, but standalone deliberately discards it (privacy)
    val js = JobLog.Succeeded(record(quasiJob, succeeded = true), secret).standalone
    val c = js.hcursor
    assert(c.get[String]("job-1").toOption.contains("work")) // identity
    assert(c.get[String]("batch").toOption.contains("batch")) // full job context (standalone)
    assert(c.get[String](JsonKeys.SUCCEEDED).toOption.exists(_.nonEmpty)) // took, under the status tag
    assert(c.downField(JsonKeys.RESULT).focus.isEmpty) // privacy: value dropped
    assert(!js.noSpaces.contains("TOP-SECRET")) // the produced value never reaches the auto-emitted log
  }

  test("standalone Unsatisfied: status tag holds took, produced value dropped even when present") {
    val js = JobLog.Unsatisfied(record(quasiJob, succeeded = false), secret).standalone
    val c = js.hcursor
    assert(c.get[String]("job-1").toOption.contains("work"))
    assert(c.get[String](JsonKeys.UNSATISFIED).toOption.exists(_.nonEmpty))
    assert(c.downField(JsonKeys.RESULT).focus.isEmpty)
    assert(!js.noSpaces.contains("TOP-SECRET"))
  }

  test("standalone Nonfatal: status tag holds took, exception message under error, no produced value") {
    val js = JobLog.Nonfatal(record(quasiJob, succeeded = false), new RuntimeException("boom")).standalone
    val c = js.hcursor
    assert(c.get[String]("job-1").toOption.contains("work"))
    assert(c.get[String](JsonKeys.NONFATAL).toOption.exists(_.nonEmpty)) // took
    assert(c.get[String](JsonKeys.ERROR).toOption.exists(_.endsWith("boom")))
    assert(c.downField(JsonKeys.RESULT).focus.isEmpty)
  }

  test("standalone Critical: status tag holds took, exception message under error, no produced value") {
    val js = JobLog.Critical(record(valueJob, succeeded = false), new RuntimeException("boom")).standalone
    val c = js.hcursor
    assert(c.get[String](JsonKeys.CRITICAL).toOption.exists(_.nonEmpty))
    assert(c.get[String](JsonKeys.ERROR).toOption.exists(_.endsWith("boom")))
    assert(c.downField(JsonKeys.RESULT).focus.isEmpty)
  }

  test("standalone Kickoff/Canceled: render the job under their lifecycle key") {
    val kickoff = JobLog.Kickoff(quasiJob).standalone
    val canceled = JobLog.Canceled(quasiJob).standalone
    assert(kickoff.hcursor.downField(JsonKeys.KICKOFF).get[String]("job-1").toOption.contains("work"))
    assert(canceled.hcursor.downField(JsonKeys.CANCELED).get[String]("job-1").toOption.contains("work"))
  }

  // ---- inBatch render (nested inside a serialized BatchResult) -------------------------------------

  test("inBatch Succeeded: job identity + status/took, and the produced value under result") {
    val js = JobLog.Succeeded(record(quasiJob, succeeded = true), secret).inBatch
    val c = js.hcursor
    assert(c.get[String]("job-1").toOption.contains("work"))
    assert(c.get[String](JsonKeys.SUCCEEDED).toOption.exists(_.nonEmpty))
    // inBatch omits the full job context that standalone merges in
    assert(c.get[String]("batch").toOption.isEmpty)
    // inBatch is the user-triggered serialization path, so the produced value is shown here
    assert(c.get[String](JsonKeys.RESULT).toOption.contains("TOP-SECRET-PRODUCED-VALUE"))
  }

  test("inBatch Succeeded with an absent value (Json.Null): the result key is dropped") {
    val js = JobLog.Succeeded(record(quasiJob, succeeded = true), Json.Null).inBatch
    val c = js.hcursor
    assert(c.get[String]("job-1").toOption.contains("work"))
    assert(c.get[String](JsonKeys.SUCCEEDED).toOption.exists(_.nonEmpty))
    assert(c.downField(JsonKeys.RESULT).focus.isEmpty) // dropNullValues removes it
  }

  test("inBatch Critical: job identity, took under critical, message under error") {
    // Critical carries no produced value, so its phantom `A` is pinned to Unit for the Encoder to resolve
    val js =
      JobLog.Critical[Unit](record(monadicJob, succeeded = false), new RuntimeException("boom")).inBatch
    val c = js.hcursor
    assert(c.get[String]("job-1").toOption.contains("work"))
    assert(c.get[String](JsonKeys.CRITICAL).toOption.exists(_.nonEmpty))
    assert(c.get[String](JsonKeys.ERROR).toOption.exists(_.endsWith("boom")))
  }

  // Note: Kickoff/Canceled extend JobLog[Nothing], so `inBatch` (which needs an Encoder[A]) is uncallable
  // for them by construction — matching the "should not happen" comment on those arms. Their real render is
  // `standalone`, covered above.
}
