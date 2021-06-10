package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionRetrying,
  ActionSucced,
  DailySummaries,
  ServiceHealthCheck,
  ServiceStarted
}
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto._

import java.time.{LocalTime, ZoneId}
import scala.concurrent.duration._

class HealthCheckTest extends AnyFunSuite {
  val guard = TaskGuard[IO]("health-check")
  test("should receive 3 health check event") {
    val a :: b :: c :: d :: rest = guard
      .updateConfig(_.withZoneId(ZoneId.of("Australia/Sydney")))
      .service("normal")
      .updateConfig(
        _.withHealthCheckInterval(1.second)
          .withStartUpDelay(1.second)
          .withDailySummaryReset(1)
          .withHealthCheckOpenTime(LocalTime.of(7, 0))
          .withHealthCheckSpan(10.hour)
          .withNormalStop)
      .eventStream(gd => gd.updateConfig(_.withExponentialBackoff(1.second)).quietly(IO.never))
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ServiceHealthCheck])
    assert(c.isInstanceOf[ServiceHealthCheck])
    assert(d.isInstanceOf[ServiceHealthCheck])
  }

  test("success") {
    val a :: b :: c :: ServiceHealthCheck(_, _, _, ds) :: rest = guard
      .service("success-test")
      .updateConfig(_.withHealthCheckInterval(1.second).withStartUpDelay(1.second))
      .eventStream(gd => gd.run(IO(1)) >> gd.loudly(IO.never))
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionSucced])
    assert(b.isInstanceOf[ServiceStarted])
    assert(c.isInstanceOf[ServiceHealthCheck])
    assert(ds.actionSucc == 1)
    assert(ds.actionRetries == 0)
    assert(ds.actionFail == 0)
    assert(ds.servicePanic == 0)
  }

  test("retry") {
    val a :: b :: c :: ServiceHealthCheck(_, _, _, ds) :: rest = guard
      .service("failure-test")
      .updateConfig(
        _.withHealthCheckInterval(1.second)
          .withStartUpDelay(1.second)
          .withConstantDelay(1.hour)
          .withDailySummaryReset(23))
      .eventStream(gd => gd.updateConfig(_.withMaxRetries(1)).run(IO.raiseError(new Exception)) >> gd.run(IO.never))
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionRetrying])
    assert(b.isInstanceOf[ServiceStarted])
    assert(c.isInstanceOf[ServiceHealthCheck])
    assert(ds.actionSucc == 0)
    assert(ds.actionRetries == 1)
    assert(ds.actionFail == 0)
    assert(ds.servicePanic == 0)
  }

  test("reset") {
    val ds = DailySummaries(1, 2, 3, 4).reset
    assert(ds.actionFail == 0)
    assert(ds.actionSucc == 0)
    assert(ds.actionRetries == 0)
    assert(ds.servicePanic == 0)
  }
}
