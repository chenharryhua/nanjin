package mtest.guard

import com.github.chenharryhua.nanjin.guard.batch.*
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import scala.concurrent.duration.DurationInt
import com.github.chenharryhua.nanjin.guard.config.{Domain, Service, Task}
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope

class BatchEncoderTest extends AnyFunSuite {
  private val batchId: BatchId = BatchId(1L)
  private val label =
    MetricScope(MetricScope.Label("batch"), Domain("test"), Service("test-service"), Task("task"))
  private val job = Job("work", 1, label, BatchMode.Sequential, Some(BatchKind.Quasi), batchId)
  private val completed = JobRecord(job, 0.millis, 12.millis, succeeded = true)
  private val failed =
    JobRecord(job.copy(kind = Some(BatchKind.Value)), 0.millis, 12.millis, succeeded = false)

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

    // each job entry is now keyed by the job's displayName, with the "succeeded" status tag as its value and
    // the took duration under its own "took" key. The batch-level record is user-triggered (not auto-logged),
    // so it also shows the produced value under "result".
    val quasiJob = quasiJson.hcursor.downField("jobs").downArray
    assert(quasiJob.get[String]("job-1 work").toOption.contains("succeeded"))
    assert(quasiJob.get[String]("took").toOption.exists(_.nonEmpty))
    assert(quasiJob.get[Int]("result").toOption.contains(1)) // the produced value

    val valueJob = valueJson.hcursor.downField("jobs").downArray
    assert(valueJob.get[String]("job-1 work").toOption.contains("succeeded"))
    assert(valueJob.get[String]("took").toOption.exists(_.nonEmpty))
    assert(valueJob.get[Int]("result").toOption.contains(1))
  }

  test("2.failed jobs carry the exception message under an \"error\" key") {
    val quasi: QuasiBatch[Int] = QuasiBatch(
      label,
      Duration.ofMillis(20),
      BatchMode.Sequential,
      batchId,
      List(JobState(failed, Left(new RuntimeException("boom")))))
    // a monadic job that threw: kind = None, and its step history carries the real Left(exception) so the
    // per-job render can classify it as "critical" (the old Right(()) sentinel could never do this).
    val monadicJob = Job("work", 1, label, BatchMode.Monadic, None, batchId)
    val monadicThrew = JobRecord(monadicJob, 0.millis, 12.millis, succeeded = false)
    val monadic: MonadicBatch[Int] =
      MonadicBatch(
        label,
        Duration.ofMillis(20),
        batchId,
        List(JobState[Unit](monadicThrew, Left(new RuntimeException("boom")))),
        Left(new RuntimeException("boom")))

    val quasiJson = quasi.asJson
    val monadicJson = monadic.asJson

    // a failed Value job is fatal: its per-job entry is keyed by the displayName with the "critical" status
    // tag as its value, the took duration under "took", and the exception message under "error"
    val quasiJob = quasiJson.hcursor.downField("jobs").downArray
    assert(quasiJob.get[String]("job-1 work").toOption.contains("critical"))
    assert(quasiJob.get[String]("took").toOption.exists(_.nonEmpty))
    assert(quasiJob.get[String]("error").toOption.exists(_.endsWith("boom")))

    // the monadic per-job entry now correctly renders the thrown step as "critical" with its message under
    // "error" (before the JobState[Unit] change the sentinel forced it to look non-thrown). The batch-level
    // failure is not serialized: the MonadicBatch encoder emits neither a top-level "result" nor "error" — a
    // failure's throwable belongs in the log entry's exception section, not the report body.
    val monadicJobJson = monadicJson.hcursor.downField("jobs").downArray
    assert(monadicJobJson.get[String]("job-1 work").toOption.contains("critical"))
    assert(monadicJobJson.get[String]("took").toOption.exists(_.nonEmpty))
    assert(monadicJobJson.get[String]("error").toOption.exists(_.endsWith("boom")))
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
    assert(allDone.allPassed)

    val withFailure = QuasiBatch(
      label,
      Duration.ofMillis(20),
      BatchMode.Sequential,
      batchId,
      List(JobState(failed, Left(new RuntimeException("x")))))
    // the batch still completed, but not every job succeeded
    assert(!withFailure.allPassed)
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
    assert(bv.allPassed)
  }

  test("5.MonadicBatch: result tracks completion; allPassed tracks per-job outcomes") {
    // chain completed (Right) but a job was rejected by its predicate
    val mb = MonadicBatch(
      label,
      Duration.ofMillis(30),
      batchId,
      List(JobState(completed, Right(())), JobState(failed, Right(()))),
      Right(99))
    assert(mb.result.isRight)
    assert(!mb.allPassed)

    // chain short-circuited by an exception
    val aborted =
      MonadicBatch(
        label,
        Duration.ofMillis(30),
        batchId,
        List(JobState(completed, Right(()))),
        Left(new RuntimeException("x")))
    assert(aborted.result.isLeft)
    assert(aborted.allPassed) // the recorded jobs all succeeded; the failure is the batch-level result
  }

  test("6.MonadicBatch encoder keys each job by its index and name") {
    val monadicJob = Job("check", 1, label, BatchMode.Monadic, None, batchId)
    // a predicate-rejected step: value produced (Right) but did not satisfy the predicate (succeeded=false)
    val monadicRejected = JobRecord(monadicJob, 0.millis, 5.millis, succeeded = false)
    val mb: MonadicBatch[Int] =
      MonadicBatch(
        label,
        Duration.ofMillis(10),
        batchId,
        List(JobState(monadicRejected, Right(()))),
        Right(0))
    val json = mb.asJson
    // the batch label is keyed by mode ("Monadic Batch"); a monadic batch has no kind
    assert(json.hcursor.get[String]("Monadic Batch").toOption.contains("batch"))
    // monadic per-job entries carry no produced value (jobs are JobState[Unit], rendered with a Json.Null
    // result that dropNullValues removes): the entry is keyed by the displayName with the "unsatisfied"
    // status tag as its value, took under its own key, and no "result"
    val jobJson = json.hcursor.downField("jobs").downArray
    assert(jobJson.get[String]("job-1 check").toOption.contains("unsatisfied"))
    assert(jobJson.get[String]("took").toOption.exists(_.nonEmpty))
    assert(jobJson.downField("result").focus.isEmpty)
    // the MonadicBatch encoder emits no top-level "result": unlike QuasiBatch/ValueBatch it does not render
    // the batch's aggregate output, so even a completed batch carries only framing and per-job outcomes
    assert(json.hcursor.downField("result").focus.isEmpty)
  }
}
