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
    assert(quasiJson.hcursor.get[String]("Sequential Quasi").toOption.contains("batch"))
    assert(valueJson.hcursor.get[String]("Sequential Value").toOption.contains("batch"))

    // each job entry is keyed "job-<index>" -> name; the produced value is never logged. In the compact
    // form the "succeeded" status tag carries the took duration string, and there is no result field.
    val quasiJob = quasiJson.hcursor.downField("jobs").downArray
    assert(quasiJob.get[String]("job-1").toOption.contains("work"))
    assert(quasiJob.get[String]("succeeded").toOption.exists(_.nonEmpty))
    assert(quasiJob.get[Int]("succeeded").toOption.isEmpty) // not the produced value

    val valueJob = valueJson.hcursor.downField("jobs").downArray
    assert(valueJob.get[String]("job-1").toOption.contains("work"))
    assert(valueJob.get[String]("succeeded").toOption.exists(_.nonEmpty))
    assert(valueJob.get[Int]("succeeded").toOption.isEmpty)
  }

  test("failed jobs carry the exception message under their status tag") {
    val quasi: QuasiBatch[Int] = QuasiBatch(
      label,
      Duration.ofMillis(20),
      BatchMode.Sequential,
      batchId,
      List(JobState(failed, Left(new RuntimeException("boom")))))
    val monadic: MonadicBatch[Int] =
      MonadicBatch(label, Duration.ofMillis(20), batchId, List(failed), Left(new RuntimeException("boom")))

    val quasiJson = quasi.asJson
    val monadicJson = monadic.asJson

    // a failed Value job is fatal: its per-job entry is keyed "job-<index>" -> name, the "critical"
    // status tag carries the took duration, and the exception message sits under "error"
    val quasiJob = quasiJson.hcursor.downField("jobs").downArray
    assert(quasiJob.get[String]("job-1").toOption.contains("work"))
    assert(quasiJob.get[String]("critical").toOption.exists(_.nonEmpty))
    assert(quasiJob.get[String]("error").toOption.exists(_.endsWith("boom")))

    // the monadic per-job entry renders the (successful) record; the batch-level failure is carried by
    // the top-level "critical" tag holding the stack trace
    assert(monadicJson.hcursor.downField("jobs").downArray.get[String]("job-1").toOption.contains("work"))
    assert(monadicJson.hcursor.downField("critical").focus.nonEmpty)
  }

  test("QuasiBatch: succeeded is always true; allPassed reflects per-job outcomes") {
    val allDone = QuasiBatch(
      label,
      Duration.ofMillis(20),
      BatchMode.Sequential,
      batchId,
      List(JobState(completed, Right(1))))
    // a quasi batch always runs to completion
    assert(allDone.succeeded)
    assert(allDone.allPassed)

    val withFailure = QuasiBatch(
      label,
      Duration.ofMillis(20),
      BatchMode.Sequential,
      batchId,
      List(JobState(failed, Left(new RuntimeException("x")))))
    // the batch still completed, but not every job succeeded
    assert(withFailure.succeeded)
    assert(!withFailure.allPassed)
  }

  test("ValueBatch: succeeded and allPassed are both true") {
    val bv =
      ValueBatch(label, Duration.ofMillis(20), BatchMode.Parallel(2), batchId, List(JobValue(completed, 1)))
    assert(bv.succeeded)
    assert(bv.allPassed)
  }

  test("MonadicBatch: succeeded tracks the result; allPassed tracks per-job outcomes") {
    // chain completed (Right) but a job was rejected by its predicate
    val mb = MonadicBatch(label, Duration.ofMillis(30), batchId, List(completed, failed), Right(99))
    assert(mb.succeeded)
    assert(!mb.allPassed)

    // chain short-circuited by an exception
    val aborted =
      MonadicBatch(label, Duration.ofMillis(30), batchId, List(completed), Left(new RuntimeException("x")))
    assert(!aborted.succeeded)
    assert(aborted.allPassed) // the recorded jobs all succeeded; the failure is the batch-level result
  }

  test("MonadicBatch encoder keys each job by its index and name") {
    val monadicJob = Job("check", 1, label, BatchMode.Monadic, None, batchId)
    val monadicFailed = JobRecord(monadicJob, 0.millis, 5.millis, succeeded = false)
    val mb: MonadicBatch[Int] =
      MonadicBatch(label, Duration.ofMillis(10), batchId, List(monadicFailed), Right(0))
    val json = mb.asJson
    // the batch label is keyed by mode ("Monadic"); a monadic batch has no kind
    assert(json.hcursor.get[String]("Monadic").toOption.contains("batch"))
    // a predicate-rejected monadic job renders via inBatch keyed "job-<index>" -> name; in the compact
    // form the "unsatisfied" status tag carries the took duration (the produced value is never logged)
    val jobJson = json.hcursor.downField("jobs").downArray
    assert(jobJson.get[String]("job-1").toOption.contains("check"))
    assert(jobJson.get[String]("unsatisfied").toOption.exists(_.nonEmpty))
  }
}
