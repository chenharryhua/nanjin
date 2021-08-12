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
    val s :: a :: b :: c :: rest = guard
      .updateConfig(_.withZoneId(ZoneId.of("Australia/Sydney")).withDailySummaryReset(1))
      .service("normal")
      .updateConfig(_.withHealthCheckInterval(1.second))
      .eventStream(gd => gd.updateConfig(_.withExponentialBackoff(1.second)).run(IO.never[Int]))
      .debug()
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ServiceHealthCheck])
    assert(c.isInstanceOf[ServiceHealthCheck])
  }

  test("success") {
    val s :: a :: b :: c :: d :: ServiceHealthCheck(_, _, _, ds) :: rest = guard
      .service("success-test")
      .updateConfig(_.withHealthCheckInterval(1.second))
      .eventStream(gd => gd.run(IO(1)) >> gd.run(IO.never))
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionSucced])
    assert(c.isInstanceOf[ActionStart])
    assert(d.isInstanceOf[ServiceHealthCheck])
  }

  test("retry") {
    val s :: a :: b :: c :: ServiceHealthCheck(_, _, _, ds) :: rest = guard
      .service("failure-test")
      .updateConfig(_.withHealthCheckInterval(1.second).withConstantDelay(1.hour))
      .eventStream(gd => gd("always-failure").max(1).run(IO.raiseError(new Exception)) >> gd.run(IO.never))
      .debug()
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ServiceHealthCheck])
  }
}
