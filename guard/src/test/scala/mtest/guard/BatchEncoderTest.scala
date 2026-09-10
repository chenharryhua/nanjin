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

  test("quasi and value batches encode kind and result tags") {
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

    assert(quasiJson.hcursor.get[String]("kind").toOption.contains("Quasi"))
    assert(quasiJson.hcursor.downField("jobs").downArray.get[Int]("result").toOption.contains(1))
    assert(valueJson.hcursor.get[String]("kind").toOption.contains("Value"))
    assert(valueJson.hcursor.downField("jobs").downArray.get[Int]("result").toOption.contains(1))
  }

  test("failed results encode with the error tag") {
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

    assert(
      quasiJson.hcursor.downField("jobs").downArray.get[String]("error").toOption.exists(_.endsWith("boom")))
    // the monadic per-job entry now renders via inBatch: a failed record is keyed "unsatisfied" with the
    // job's displayName; the exception itself is carried by the batch-level "error" tag.
    assert(
      monadicJson.hcursor
        .downField("jobs")
        .downArray
        .get[String]("unsatisfied")
        .toOption
        .exists(_.contains("work")))
    assert(monadicJson.hcursor.get[List[String]]("error").toOption.exists(_.exists(_.contains("boom"))))
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

  test("MonadicBatch encoder renders a failed job as unsatisfied") {
    val monadicJob = Job("check", 1, label, BatchMode.Monadic, None, batchId)
    val monadicFailed = JobRecord(monadicJob, 0.millis, 5.millis, succeeded = false)
    val mb: MonadicBatch[Int] =
      MonadicBatch(label, Duration.ofMillis(10), batchId, List(monadicFailed), Right(0))
    val json = mb.asJson
    val jobJson = json.hcursor.downField("jobs").downArray
    // a failed monadic job renders via inBatch, keyed "unsatisfied" with the job's displayName
    assert(jobJson.get[String]("unsatisfied").toOption.exists(_.contains("check")))
  }
}
