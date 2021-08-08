package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionRetrying,
  ActionStart,
  ActionSucced,
  DailySummaries,
  ServiceHealthCheck,
  ServiceStarted
}
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalTime, ZoneId}
import scala.concurrent.duration.*

class HealthCheckTest extends AnyFunSuite {
  val guard = TaskGuard[IO]("health-check")
  test("should receive 3 health check event") {
    val a :: b :: c :: d :: rest = guard
      .updateConfig(_.withZoneId(ZoneId.of("Australia/Sydney")).withDailySummaryReset(1))
      .service("normal")
      .updateConfig(
        _.withHealthCheckInterval(1.second)
          .withStartupDelay(1.second)
          .withHealthCheckOpenTime(LocalTime.of(7, 0))
          .withHealthCheckSpan(10.hour))
      .eventStream(gd => gd.updateConfig(_.withExponentialBackoff(1.second)).quietly(IO.never[Int]))
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ServiceStarted])
    assert(c.isInstanceOf[ServiceHealthCheck])
    assert(d.isInstanceOf[ServiceHealthCheck])
  }

  test("success") {
    val a :: b :: c :: d :: e :: ServiceHealthCheck(_, _, _, ds, _, _) :: rest = guard
      .service("success-test")
      .updateConfig(_.withHealthCheckInterval(1.second).withStartupDelay(1.second))
      .eventStream(gd => gd.run(IO(1)) >> gd.loudly(IO.never))
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionSucced])
    assert(c.isInstanceOf[ActionStart])
    assert(d.isInstanceOf[ServiceStarted])
    assert(e.isInstanceOf[ServiceHealthCheck])
  }

  test("retry") {
    val a :: b :: c :: d :: ServiceHealthCheck(_, _, _, ds, _, _) :: rest = guard
      .service("failure-test")
      .updateConfig(_.withHealthCheckInterval(1.second).withStartupDelay(1.second).withConstantDelay(1.hour))
      .eventStream(gd => gd("always-failure").max(1).run(IO.raiseError(new Exception)) >> gd.run(IO.never))
      .debug()
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ServiceStarted])
    assert(d.isInstanceOf[ServiceHealthCheck])
  }

}
