package mtest.guard

import com.github.chenharryhua.nanjin.guard.batch.*
import com.github.chenharryhua.nanjin.guard.event.MetricLabel
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import java.util.UUID
import com.github.chenharryhua.nanjin.guard.config.Domain

class BatchEncoderTest extends AnyFunSuite {
  private val batchId = UUID.fromString("00000000-0000-0000-0000-000000000001")
  private val label = MetricLabel("batch", Domain("test"))
  private val job = Job("work", 1, label, BatchMode.Sequential, BatchKind.Quasi, batchId)
  private val completed = CompletedJob(job, Duration.ofMillis(12), done = true)
  private val failed = CompletedJob(job.copy(kind = BatchKind.Value), Duration.ofMillis(12), done = false)

  test("quasi and value batches encode kind and result tags") {
    val quasi = QuasiBatch(
      label,
      Duration.ofMillis(20),
      BatchMode.Sequential,
      batchId,
      List(JobState(completed, Right(1))))
    val value =
      BatchValue(label, Duration.ofMillis(20), BatchMode.Sequential, batchId, List(JobValue(completed, 1)))

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
    assert(
      monadicJson.hcursor.downField("jobs").downArray.get[String]("failed").toOption.contains("critical"))
    assert(monadicJson.hcursor.get[List[String]]("error").toOption.exists(_.exists(_.contains("boom"))))
  }
}
