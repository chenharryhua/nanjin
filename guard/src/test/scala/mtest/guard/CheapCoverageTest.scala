package mtest.guard

import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.batch.{BatchId, BatchMode, Job, JobFlag, JobRecord, JobState}
import com.github.chenharryhua.nanjin.guard.config.{Domain, HostName, LaunchTime, LogFormat, Service, Task}
import com.github.chenharryhua.nanjin.guard.event.Message
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope
import io.circe.Json
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import munit.FunSuite

import java.time.{ZoneId, ZonedDateTime}
import scala.concurrent.duration.DurationInt

/** Pure, offline coverage of small `Show`/extension/encoder methods and the `HostName.value` match arms
  * (reached by decoding JSON rather than probing EC2). Production source is not modified.
  */
class CheapCoverageTest extends FunSuite {

  // --- O3: Show / extension / encoder ---

  test("1.BatchMode.Monadic renders via its Show instance") {
    assertEquals(BatchMode.Monadic.show, "Monadic")
  }

  test("2.LogFormat renders its product prefix via Show") {
    assertEquals(LogFormat.ConsolePlainText.show, "ConsolePlainText")
  }

  test("3.Message renders its JSON via Show") {
    val msg = Message(Json.obj("k" -> Json.fromInt(1)))
    assertEquals(msg.show, Json.obj("k" -> Json.fromInt(1)).spaces2)
  }

  test("4.LaunchTime.instant returns the underlying instant") {
    val zdt = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneId.of("Etc/UTC"))
    assertEquals(LaunchTime(zdt).instant, zdt.toInstant)
  }

  test("5.JobState.succeeded reflects flag and result") {
    val scope = MetricScope(MetricScope.Label("label"), Domain("domain"), Service("svc"), Task("task"))
    val job = Job("job", 1, scope, BatchMode.Monadic, None, BatchId(1L))
    val record = JobRecord(job, 0.millis, 1.millis, None)

    val accepted = JobState(record, JobFlag.Accepted, Right(1))
    val unmet = JobState(record, JobFlag.Unmet, Right(1))
    val failed = JobState(record, JobFlag.Failed, Right(1))
    val leftAccepted = JobState(record, JobFlag.Accepted, Left(new Exception("boom")))

    assert(accepted.succeeded)
    assert(!unmet.succeeded)
    assert(!failed.succeeded)
    assert(!leftAccepted.succeeded)
  }

  // --- O4: HostName.value match arms via decode ---

  test("6.HostName.value is 'unknown' when both ec2 and local are absent") {
    val hn = decode[HostName]("""{"ec2":null,"local":null}""").toOption.get
    assertEquals(hn.value, "unknown")
    assertEquals(hn.show, "unknown")
  }

  test("7.HostName.value is the ec2 value when only ec2 is present") {
    val hn = decode[HostName]("""{"ec2":"1.2.3.4","local":null}""").toOption.get
    assertEquals(hn.value, "1.2.3.4")
  }

  test("8.HostName.value joins ec2 and local when both are present") {
    val hn = decode[HostName]("""{"ec2":"a","local":"b"}""").toOption.get
    assertEquals(hn.value, "a/b")
  }

  test("9.HostName round-trips through its codec") {
    val hn = decode[HostName]("""{"ec2":"a","local":"b"}""").toOption.get
    assertEquals(decode[HostName](hn.asJson.noSpaces).toOption, Some(hn))
  }
}
