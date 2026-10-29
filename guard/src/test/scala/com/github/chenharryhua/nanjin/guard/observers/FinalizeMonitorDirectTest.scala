package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.{ReportedEvent, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.StopReason
import munit.CatsEffectSuite

class FinalizeMonitorDirectTest extends CatsEffectSuite {

  private def events(name: String): IO[List[Event]] =
    TaskGuard[IO]("finalize-monitor")
      .service(name)
      .updateConfig(_.withReportedEventThreshold(_.Info))
      .eventStream(_.logger.info("message"))
      .compile
      .toList

  test("1.a tracked start produces one cancellation stop and terminated drains the state") {
    events("unfinished").flatMap { source =>
      val start = source.collectFirst { case event: ServiceStart => event }.get
      val reported = source.collectFirst { case event: ReportedEvent => event }.get

      for {
        monitor <- FinalizeMonitor[IO]
        _ <- monitor.monitoring(reported) // unrelated events do not change tracking
        before_start <- monitor.terminated
        _ <- monitor.monitoring(start)
        terminated <- monitor.terminated
        drained <- monitor.terminated
      } yield {
        assertEquals(before_start.toList, Nil)
        assertEquals(terminated.size, 1)
        terminated.toList.head match {
          case stop: ServiceStop =>
            assertEquals(stop.serviceIdentity, start.serviceIdentity)
            assertEquals(stop.brief, start.brief)
            assertEquals(stop.logLink, None)
            assertEquals(stop.cause, StopReason.ByCancellation)
          case other => fail(s"expected ServiceStop, got $other")
        }
        assertEquals(drained.toList, Nil)
      }
    }
  }

  test("2.a normal stop removes its matching start") {
    events("completed").flatMap { source =>
      val start = source.collectFirst { case event: ServiceStart => event }.get
      val stop = source.collectFirst { case event: ServiceStop => event }.get

      for {
        monitor <- FinalizeMonitor[IO]
        _ <- monitor.monitoring(start)
        _ <- monitor.monitoring(stop)
        terminated <- monitor.terminated
      } yield assertEquals(terminated.toList, Nil)
    }
  }

  test("3.multiple services are tracked independently") {
    for {
      first_events <- events("first")
      second_events <- events("second")
      first_start = first_events.collectFirst { case event: ServiceStart => event }.get
      first_stop = first_events.collectFirst { case event: ServiceStop => event }.get
      second_start = second_events.collectFirst { case event: ServiceStart => event }.get
      monitor <- FinalizeMonitor[IO]
      _ <- monitor.monitoring(first_start)
      _ <- monitor.monitoring(second_start)
      _ <- monitor.monitoring(first_stop)
      terminated <- monitor.terminated
    } yield {
      val stops = terminated.toList.collect { case stop: ServiceStop => stop }
      assertEquals(stops.size, 1)
      assertEquals(stops.head.serviceIdentity.serviceId, second_start.serviceIdentity.serviceId)
    }
  }

  test("4.a repeated start for one service id remains one tracked service") {
    events("restarted").flatMap { source =>
      val start = source.collectFirst { case event: ServiceStart => event }.get
      val restart = start.copy(tick = start.tick.copy(index = start.tick.index + 1L))

      for {
        monitor <- FinalizeMonitor[IO]
        _ <- monitor.monitoring(start)
        _ <- monitor.monitoring(restart)
        terminated <- monitor.terminated
      } yield assertEquals(terminated.size, 1)
    }
  }
}
