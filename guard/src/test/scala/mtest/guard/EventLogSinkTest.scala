package mtest.guard

import cats.Endo
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.ServiceConfig
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
      .updateConfig(_.withRestartPolicy(10.hour, _.fixedDelay(2.seconds).limited(1)))

  private def stream(configure: Endo[ServiceConfig[IO]]): List[Event] =
    base
      .updateConfig(configure)
      .eventStream(_ => IO.unit)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

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

  test("2.Console_PlainText") {
    val events = stream(_.withLogFormat(_.Console_PlainText))
    assertStartStop(events)
  }

  test("3.Console_Json") {
    val events = stream(_.withLogFormat(_.Console_Json))
    assertStartStop(events)
  }

  test("4.Console_Json_NoColor") {
    val events = stream(_.withLogFormat(_.Console_Json_NoColor))
    assertStartStop(events)
  }

  test("5.Console_Json_MultiLine") {
    val events = stream(_.withLogFormat(_.Console_Json_MultiLine))
    assertStartStop(events)
  }

  test("6.Console_Json_Verbose") {
    val events = stream(_.withLogFormat(_.Console_Json_Verbose))
    assertStartStop(events)
  }

  test("7.Slf4j_PlainText") {
    val events = stream(_.withLogFormat(_.Slf4j_PlainText))
    assertStartStop(events)
  }

  test("8.Slf4j_Json") {
    val events = stream(_.withLogFormat(_.Slf4j_Json))
    assertStartStop(events)
  }

  test("9.Slf4j_Json_NoColor") {
    val events = stream(_.withLogFormat(_.Slf4j_Json_NoColor))
    assertStartStop(events)
  }

  test("10.log format does not suppress events on service panic") {
    val events = base
      .updateConfig(
        _.withLogFormat(_.Console_Json)
          .withRestartPolicy(10.hour, _.fixedDelay(100.millis).limited(1)))
      .eventStream(_ => IO.raiseError(new Exception("boom")))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()
    assert(events.exists(_.isInstanceOf[Event.ServicePanic]))
    assert(events.last.isInstanceOf[ServiceStop])
  }

  test("11.reported events appear in stream under Console_Json") {
    val events = base
      .updateConfig(_.withLogFormat(_.Console_Json).withInitialLogLevel(_.Info))
      .eventStream(agent => agent.herald.info("hello"))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()
    assert(events.exists(_.isInstanceOf[Event.ReportedEvent]))
  }

  test("12.reported events appear in stream under Slf4j_Json") {
    val events = base
      .updateConfig(_.withLogFormat(_.Slf4j_Json).withInitialLogLevel(_.Info))
      .eventStream(agent => agent.herald.warn("attention"))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()
    assert(events.exists(_.isInstanceOf[Event.ReportedEvent]))
  }

  test("13.metrics snapshot included when adhoc report triggered") {
    val events = base
      .updateConfig(_.withLogFormat(_.Console_Json))
      .eventStream(agent => agent.adhoc.report)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()
    assert(events.exists(_.isInstanceOf[Event.MetricsSnapshot]))
  }
}
