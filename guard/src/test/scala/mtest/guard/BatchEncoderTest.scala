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

  test("quasi and value batches key the label by mode+kind and render each job under its status tag") {
    val quasi = QuasiBatch(
      label,
      Duration.ofMillis(20),
      BatchMode.Sequential,
      batchId,
      List(JobState(completed, Right(1))))
    val value =
      ValueBatch(label, Duration.ofMillis(20), BatchMode.Sequential, batchId, List(JobValue(completed, 1)))

    val quasiJson = quasi.asJson
    val valueJson = value.asJson

    // the batch label now lives under a mode+kind key rather than separate "batch"/"mode"/"kind" fields
    assert(quasiJson.hcursor.get[String]("Sequential Quasi Batch").toOption.contains("batch"))
    assert(valueJson.hcursor.get[String]("Sequential Value Batch").toOption.contains("batch"))

    // QuasiBatch outcome counts use "passed"/"failed" (integer tallies), named distinctly from the
    // per-job "succeeded" status tag which carries a took duration
    assert(quasiJson.hcursor.get[Int]("passed").toOption.contains(1))
    assert(quasiJson.hcursor.get[Int]("failed").toOption.contains(0))

    // each job entry is keyed "job-<index>" -> name; the compact "succeeded" status tag carries the took
    // duration string. The batch-level record is user-triggered (not auto-logged), so it also shows the
    // produced value under "result".
    val quasiJob = quasiJson.hcursor.downField("jobs").downArray
    assert(quasiJob.get[String]("job-1").toOption.contains("work"))
    assert(quasiJob.get[String]("succeeded").toOption.exists(_.nonEmpty)) // took, not the value
    assert(quasiJob.get[Int]("result").toOption.contains(1)) // the produced value

    val valueJob = valueJson.hcursor.downField("jobs").downArray
    assert(valueJob.get[String]("job-1").toOption.contains("work"))
    assert(valueJob.get[String]("succeeded").toOption.exists(_.nonEmpty))
    assert(valueJob.get[Int]("result").toOption.contains(1))
  }

  test("failed jobs carry the exception message under their status tag") {
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

    // a failed Value job is fatal: its per-job entry is keyed "job-<index>" -> name, the "critical"
    // status tag carries the took duration, and the exception message sits under "error"
    val quasiJob = quasiJson.hcursor.downField("jobs").downArray
    assert(quasiJob.get[String]("job-1").toOption.contains("work"))
    assert(quasiJob.get[String]("critical").toOption.exists(_.nonEmpty))
    assert(quasiJob.get[String]("error").toOption.exists(_.endsWith("boom")))

    // the monadic per-job entry now correctly renders the thrown step under "critical"/"error" (before the
    // JobState[Unit] change the sentinel forced it to look non-thrown); the batch-level failure is carried
    // by the top-level "error" tag holding the stack trace (a successful monadic batch would use "result")
    val monadicJobJson = monadicJson.hcursor.downField("jobs").downArray
    assert(monadicJobJson.get[String]("job-1").toOption.contains("work"))
    assert(monadicJobJson.get[String]("critical").toOption.exists(_.nonEmpty))
    assert(monadicJobJson.get[String]("error").toOption.exists(_.endsWith("boom")))
    assert(monadicJson.hcursor.downField("error").focus.nonEmpty)
  }

  test("QuasiBatch: allPassed reflects per-job outcomes") {
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

  test("ValueBatch: allPassed is true") {
    val bv =
      ValueBatch(label, Duration.ofMillis(20), BatchMode.Parallel(2), batchId, List(JobValue(completed, 1)))
    assert(bv.allPassed)
  }

  test("MonadicBatch: result tracks completion; allPassed tracks per-job outcomes") {
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

  test("MonadicBatch encoder keys each job by its index and name") {
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
    // result that dropNullValues removes): keyed "job-<index>" -> name with the "unsatisfied" took tag only
    val jobJson = json.hcursor.downField("jobs").downArray
    assert(jobJson.get[String]("job-1").toOption.contains("check"))
    assert(jobJson.get[String]("unsatisfied").toOption.exists(_.nonEmpty))
    assert(jobJson.downField("result").focus.isEmpty)
    // a completed monadic batch shows its final result (the user's declared output) under "result"
    assert(json.hcursor.get[Int]("result").toOption.contains(0))
  }
}
