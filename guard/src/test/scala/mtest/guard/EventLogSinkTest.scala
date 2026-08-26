package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.{ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.StopReason.Successfully
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.EventLogSinkTest"
class EventLogSinkTest extends AnyFunSuite {

  private val base =
    TaskGuard[IO]("nanjin")
      .service("event-log-sink")
      .updateConfig(_.withRestartPolicy(10.hour, _.fixedDelay(2.seconds).repeat.limited(1)))

  private def assertStartStop(events: List[Event]): Unit = {
    assert(events.size == 2)
    assert(events.head.isInstanceOf[ServiceStart])
    assert(events.last.asInstanceOf[ServiceStop].cause == Successfully): Unit
  }

  test("1.no log format (None) - no-op sink, events still emitted") {
    val events = base
      .eventStream(_ => IO.unit)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()
    assertStartStop(events)
  }

  test("2.log format does not suppress events on service panic") {
    val events = base
      .updateConfig(_.withRestartPolicy(10.hour, _.fixedDelay(100.millis).repeat.limited(1)))
      .eventStream(_ => IO.raiseError(new Exception("boom")))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()
    assert(events.exists(_.isInstanceOf[Event.ServicePanic]))
    assert(events.last.isInstanceOf[ServiceStop])
  }

  test("3.metrics snapshot included when adhoc report triggered") {
    val events = base
      .eventStream(agent => agent.adhoc.report)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()
    assert(events.exists(_.isInstanceOf[Event.MetricsSnapshot]))
  }
}
