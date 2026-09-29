package mtest.guard

import com.github.chenharryhua.nanjin.guard.batch.*
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.FunSuite

import java.time.Duration
import scala.concurrent.duration.DurationInt
import com.github.chenharryhua.nanjin.guard.config.{Domain, Service, Task}
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope

class BatchEncoderTest extends FunSuite {
  private val batchId: BatchId = BatchId(1L)
  private val label =
    MetricScope(MetricScope.Label("batch"), Domain("test"), Service("test-service"), Task("task"))
  private val job = Job("work", 1, label, BatchMode.Sequential, Some(BatchKind.Quasi), batchId)
  private val completed = JobRecord(job, 0.millis, 12.millis, isValid = true)
  private val failed =
    JobRecord(job.copy(kind = Some(BatchKind.Value)), 0.millis, 12.millis, isValid = false)

  test("1.quasi and value batches key the label by mode+kind and render each job by its displayName") {
    val quasi = QuasiBatch(
      label,
      Duration.ofMillis(20),
      BatchMode.Sequential,
      batchId,
      List(JobState(completed, Right(1))))
    val value =
      ValueBatch(
        label,
        Duration.ofMillis(20),
        BatchMode.Sequential,
        batchId,
        List(JobState(completed, Right(1))),
        List(1))

    val quasiJson = quasi.asJson
    val valueJson = value.asJson

    // the batch label now lives under a mode+kind key rather than separate "batch"/"mode"/"kind" fields
    assert(quasiJson.hcursor.get[String]("Sequential Quasi Batch").toOption.contains("batch"))
    assert(valueJson.hcursor.get[String]("Sequential Value Batch").toOption.contains("batch"))

    // QuasiBatch outcome counts use "passed"/"failed" (integer tallies), named distinctly from the
    // per-job "succeeded" status tag which carries a took duration
    assert(quasiJson.hcursor.get[Int]("passed").toOption.contains(1))
    assert(quasiJson.hcursor.get[Int]("failed").toOption.contains(0))

    // Each job entry renders its identity, the "succeeded" status tag, the took duration, and (on the
    // user-triggered batch path) the produced value. The exact key layout is UI-facing and may evolve, so
    // assert those facts by presence rather than by fixed keys.
    val quasiJob = quasiJson.hcursor.downField("jobs").downArray.focus.get.noSpaces
    assert(quasiJob.contains("job-1") && quasiJob.contains("work")) // identity
    assert(quasiJob.contains("succeeded")) // status tag
    assert(quasiJob.contains("12 milli")) // took
    assert(quasiJob.contains("\"succeeded\":1")) // the produced value

    val valueJob = valueJson.hcursor.downField("jobs").downArray.focus.get.noSpaces
    assert(valueJob.contains("job-1") && valueJob.contains("work"))
    assert(valueJob.contains("succeeded"))
    assert(valueJob.contains("12 milli"))
    assert(valueJob.contains("\"succeeded\":1"))
  }

  test("2.failed jobs carry the exception message under the outcome tag") {
    val quasi: QuasiBatch[Int] = QuasiBatch(
      label,
      Duration.ofMillis(20),
      BatchMode.Sequential,
      batchId,
      List(JobState(failed, Left(new RuntimeException("boom")))))
    // A monadic job that threw: kind = None, and its step history carries the real Left(exception) so the
    // per-job render can classify it as "critical".
    val monadicJob = Job("work", 1, label, BatchMode.Monadic, None, batchId)
    val monadicThrew = JobRecord(monadicJob, 0.millis, 12.millis, isValid = false)
    val monadic: MonadicBatch[Int] =
      MonadicBatch(
        label,
        Duration.ofMillis(20),
        batchId,
        List(JobState[Json](monadicThrew, Left(new RuntimeException("boom")))),
        Left(new RuntimeException("boom")))

    val quasiJson = quasi.asJson
    val monadicJson = monadic.asJson

    // a failed Value job is fatal: its per-job entry renders identity, the "critical" status tag, took, and
    // the exception message. Assert by presence rather than fixed keys (UI-facing layout may evolve).
    val quasiJob = quasiJson.hcursor.downField("jobs").downArray.focus.get.noSpaces
    assert(quasiJob.contains("job-1") && quasiJob.contains("work"))
    assert(quasiJob.contains("critical"))
    assert(quasiJob.contains("12 milli"))
    assert(quasiJob.contains("boom"))

    // the monadic per-job entry renders the thrown step as "critical" with its message. The batch-level
    // failure is not serialized: the MonadicBatch encoder emits no top-level "result" or "error" — a
    // failure's throwable belongs in the log entry's exception section, not the report body.
    val monadicJobJson = monadicJson.hcursor.downField("jobs").downArray.focus.get.noSpaces
    assert(monadicJobJson.contains("job-1") && monadicJobJson.contains("work"))
    assert(monadicJobJson.contains("critical"))
    assert(monadicJobJson.contains("boom"))
    assert(monadicJson.hcursor.downField("error").focus.isEmpty)
  }

  test("3.QuasiBatch: allPassed reflects per-job outcomes") {
    val allDone = QuasiBatch(
      label,
      Duration.ofMillis(20),
      BatchMode.Sequential,
      batchId,
      List(JobState(completed, Right(1))))
    // every job satisfied its post-condition
    assert(allDone.allSucceeded)

    val withFailure = QuasiBatch(
      label,
      Duration.ofMillis(20),
      BatchMode.Sequential,
      batchId,
      List(JobState(failed, Left(new RuntimeException("x")))))
    // the batch still completed, but not every job succeeded
    assert(!withFailure.allSucceeded)
  }

  test("4.ValueBatch: allPassed is true") {
    val bv =
      ValueBatch(
        label,
        Duration.ofMillis(20),
        BatchMode.Parallel(2),
        batchId,
        List(JobState(completed, Right(1))),
        List(1))
    assert(bv.allSucceeded)
  }

  test("5.MonadicBatch: result tracks completion; allPassed tracks per-job outcomes") {
    // chain completed (Right) but a job was rejected by its predicate
    val mb = MonadicBatch(
      label,
      Duration.ofMillis(30),
      batchId,
      List(JobState(completed, Right(Json.Null)), JobState(failed, Right(Json.Null))),
      Right(99))
    assert(mb.result.isRight)
    assert(!mb.allSucceeded)

    // chain short-circuited by an exception
    val aborted =
      MonadicBatch(
        label,
        Duration.ofMillis(30),
        batchId,
        List(JobState(completed, Right(Json.Null))),
        Left(new RuntimeException("x")))
    assert(aborted.result.isLeft)
    assert(aborted.allSucceeded) // the recorded jobs all succeeded; the failure is the batch-level result
  }

  test("6.MonadicBatch encoder keys each job by its index and name") {
    val monadicJob = Job("check", 1, label, BatchMode.Monadic, None, batchId)
    // a predicate-rejected step: value produced (Right) but did not satisfy the predicate (succeeded=false)
    val monadicRejected = JobRecord(monadicJob, 0.millis, 5.millis, isValid = false)
    val mb: MonadicBatch[Int] =
      MonadicBatch(
        label,
        Duration.ofMillis(10),
        batchId,
        List(JobState(monadicRejected, Right(Json.Null))),
        Right(0))
    val json = mb.asJson
    // the batch label is keyed by mode ("Monadic Batch"); a monadic batch has no kind
    assert(json.hcursor.get[String]("Monadic Batch").toOption.contains("batch"))
    // Untranslated monadic per-job entries default to Json.Null. The entry renders its identity, the
    // "unsatisfied" status tag with a null payload, and took, with no "result". Assert by presence;
    // the exact key layout is UI-facing and may evolve.
    val jobEntry = json.hcursor.downField("jobs").downArray
    val jobJson = jobEntry.focus.get.noSpaces
    assert(jobJson.contains("job-1") && jobJson.contains("check"))
    assert(jobJson.contains("unsatisfied"))
    assert(jobJson.contains("5 milli")) // took (record uses 5.millis)
    assertEquals(jobEntry.downField("unsatisfied").focus, Some(Json.Null))
    assert(jobEntry.downField("result").focus.isEmpty)
    // Successful monadic batches render the aggregate value; untranslated job entries omit their null payload.
    assert(json.hcursor.get[Int]("result").toOption.contains(0))
  }
}
