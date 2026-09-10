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
  private val job = Job("work", 1, label, BatchMode.Sequential, BatchKind.Quasi, batchId)
  private val completed = JobRecord(job, 0.millis, 12.millis, succeeded = true)
  private val failed =
    JobRecord(job.copy(kind = BatchKind.Value), 0.millis, 12.millis, succeeded = false)

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
    assert(monadicJson.hcursor.downField("jobs").downArray.get[String]("job-1").toOption.contains("work"))
    // the monadic summary no longer carries per-job severity; a failed job is flagged with "failed": true
    assert(monadicJson.hcursor.downField("jobs").downArray.get[Boolean]("failed").toOption.contains(true))
    assert(monadicJson.hcursor.get[List[String]]("error").toOption.exists(_.exists(_.contains("boom"))))
  }

  test("CompletedBatch encoder produces correct JSON") {
    val cb = CompletedBatch(
      scope = label,
      spent = Duration.ofMillis(50),
      mode = BatchMode.Sequential,
      batchId = batchId,
      jobs = List(completed, failed)
    )
    val json = cb.asJson
    assert(json.hcursor.get[String]("batch").toOption.contains("batch"))
    assert(json.hcursor.get[String]("mode").toOption.contains("Sequential"))
    assert(json.hcursor.get[Int]("succeeded").toOption.contains(1))
    assert(json.hcursor.get[Int]("failed").toOption.contains(1))
    val jobsArr = json.hcursor.downField("jobs").as[List[io.circe.Json]].toOption.get
    assert(jobsArr.size == 2)
    assert(jobsArr.head.hcursor.get[Boolean]("succeeded").toOption.contains(true))
    assert(jobsArr(1).hcursor.get[Boolean]("succeeded").toOption.contains(false))
  }

  test("CompletedBatch.done returns true when all jobs done") {
    val cb = CompletedBatch(label, Duration.ofMillis(10), BatchMode.Sequential, batchId, List(completed))
    assert(cb.succeeded)
  }

  test("CompletedBatch.done returns false when any job failed") {
    val cb =
      CompletedBatch(label, Duration.ofMillis(10), BatchMode.Sequential, batchId, List(completed, failed))
    assert(!cb.succeeded)
  }

  test("QuasiBatch.done and summary accessors") {
    val allDone = QuasiBatch(
      label,
      Duration.ofMillis(20),
      BatchMode.Sequential,
      batchId,
      List(JobState(completed, Right(1))))
    assert(allDone.succeeded)
    val cb = allDone.summary
    assert(cb.scope == label)
    assert(cb.jobs.size == 1)
    assert(cb.succeeded)

    val withFailure = QuasiBatch(
      label,
      Duration.ofMillis(20),
      BatchMode.Sequential,
      batchId,
      List(JobState(failed, Left(new RuntimeException("x")))))
    assert(!withFailure.succeeded)
    assert(!withFailure.summary.succeeded)
  }

  test("ValueBatch.summary accessor") {
    val bv =
      ValueBatch(label, Duration.ofMillis(20), BatchMode.Parallel(2), batchId, List(JobValue(completed, 1)))
    assert(bv.succeeded)
    val cb = bv.summary
    assert(cb.scope == label)
    assert(cb.mode == BatchMode.Parallel(2))
    assert(cb.jobs.size == 1)
    assert(cb.succeeded)
  }

  test("MonadicBatch.summary accessor") {
    val mb = MonadicBatch(label, Duration.ofMillis(30), batchId, List(completed, failed), Right(99))
    assert(mb.succeeded)
    val cb = mb.summary
    assert(cb.scope == label)
    assert(cb.mode == BatchMode.Monadic)
    assert(cb.jobs.size == 2)
  }

  test("MonadicBatch encoder flags a failed job without severity") {
    val monadicJob = Job("check", 1, label, BatchMode.Monadic, BatchKind.Value, batchId)
    val monadicFailed = JobRecord(monadicJob, 0.millis, 5.millis, succeeded = false)
    val mb: MonadicBatch[Int] =
      MonadicBatch(label, Duration.ofMillis(10), batchId, List(monadicFailed), Right(0))
    val json = mb.asJson
    val jobJson = json.hcursor.downField("jobs").downArray
    // monadic jobs are all Value and the summary carries no per-job severity
    assert(jobJson.get[Boolean]("failed").toOption.contains(true))
    assert(jobJson.get[String]("job-1").toOption.contains("check"))
  }
}
