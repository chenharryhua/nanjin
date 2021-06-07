package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.alert.{DailySummaries, ServiceHealthCheck, ServiceStarted}
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto._

import java.time.ZoneId
import scala.concurrent.duration._

class HealthCheckTest extends AnyFunSuite {
  val guard = TaskGuard[IO]("health-check")
  test("should receive 3 health check event") {
    val Vector(a, b, c, d) = guard
      .service("normal")
      .updateServiceConfig(
        _.withHealthCheckInterval(1.second)
          .withStartUpDelay(1.second)
          .withZoneId(ZoneId.of("Australia/Sydney"))
          .withDailySummaryReset(1))
      .eventStream(gd => gd.run(IO.never))
      .interruptAfter(5.second)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ServiceHealthCheck])
    assert(c.isInstanceOf[ServiceHealthCheck])
    assert(d.isInstanceOf[ServiceHealthCheck])
  }

  test("success") {
    val Vector(_, a, b, c, ServiceHealthCheck(_, ds)) = guard
      .service("success-test")
      .updateServiceConfig(_.withHealthCheckInterval(1.second).withStartUpDelay(1.second))
      .eventStream(gd => gd.run(IO(1)) >> gd.run(IO.never))
      .interruptAfter(5.second)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ServiceHealthCheck])
    assert(c.isInstanceOf[ServiceHealthCheck])
    assert(ds.actionSucc == 1)
    assert(ds.actionRetries == 0)
    assert(ds.actionFail == 0)
    assert(ds.servicePanic == 0)
  }

  test("retry") {
    val Vector(_, a, b, c, ServiceHealthCheck(_, ds)) = guard
      .service("failure-test")
      .updateServiceConfig(
        _.withHealthCheckInterval(1.second)
          .withStartUpDelay(1.second)
          .withConstantDelay(1.hour)
          .withDailySummaryReset(23))
      .eventStream(gd =>
        gd.updateActionConfig(_.withMaxRetries(1)).run(IO.raiseError(new Exception)) >> gd.run(IO.never))
      .interruptAfter(5.second)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ServiceHealthCheck])
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
