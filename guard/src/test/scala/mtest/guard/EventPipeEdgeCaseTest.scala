package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event.*
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index.{Adhoc, Periodic}
import com.github.chenharryhua.nanjin.guard.event.{Event, EventPipe}
import cats.syntax.order.catsSyntaxPartialOrder
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class EventPipeEdgeCaseTest extends AnyFunSuite {

  private val service =
    TaskGuard[IO]("pipe.edge")
      .service("pipe.edge")
      .updateConfig(_.withMetricsReport(_.crontab(_.secondly)).withInitialLogLevel(_.Debug))

  // --- identity ---

  test("1.EventPipe.identity passes all events through unchanged") {
    val events = service
      .eventStream(agent => agent.herald.info("msg") *> agent.adhoc.report)
      .map(checkJson)
      .filter(EventPipe.identity.filter)
      .compile
      .toList
      .unsafeRunSync()

    assert(events.nonEmpty)
    assert(events.head.isInstanceOf[ServiceStart])
    assert(events.last.isInstanceOf[ServiceStop])
  }

  // --- && composition ---

  test("2.EventPipe.&& composes two filters") {
    // indexFilter(2) keeps index=2,4,6... and windowFilter(3.seconds) keeps ~3,6,9...
    // Their intersection should be narrower than either alone
    val all = service
      .eventStream(_ => IO.sleep(7.seconds))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val byIndex = all.filter(EventPipe.indexFilter(3).filter)
    val byWindow = all.filter(EventPipe.windowFilter(3.seconds).filter)
    val byBoth = all.filter((EventPipe.indexFilter(3) && EventPipe.windowFilter(3.seconds)).filter)

    // composed filter should produce <= results of either individual filter for MetricsSnapshot
    val bothMetrics = byBoth.count(_.isInstanceOf[MetricsSnapshot])
    val indexMetrics = byIndex.count(_.isInstanceOf[MetricsSnapshot])
    val windowMetrics = byWindow.count(_.isInstanceOf[MetricsSnapshot])
    assert(bothMetrics <= indexMetrics)
    assert(bothMetrics <= windowMetrics)
  }

  // --- logLevel ---

  test("3.EventPipe.logLevel filters ReportedEvents below threshold") {
    val events = service
      .eventStream { agent =>
        agent.herald.debug("debug-msg") *>
          agent.herald.info("info-msg") *>
          agent.herald.warn("warn-msg") *>
          agent.herald.error("error-msg")
      }
      .map(checkJson)
      .filter(EventPipe.logLevel(_.Warn).filter)
      .compile
      .toList
      .unsafeRunSync()

    val reported = events.collect { case r: ReportedEvent => r }
    // Only warn and error should pass (debug and info should be filtered)
    assert(reported.forall(_.level >= LogLevel.Warn))
    assert(reported.size == 2)
  }

  test("4.EventPipe.logLevel passes non-ReportedEvent events through") {
    val events = service
      .eventStream(agent => agent.herald.debug("debug") *> agent.adhoc.report)
      .map(checkJson)
      .filter(EventPipe.logLevel(_.Error).filter)
      .compile
      .toList
      .unsafeRunSync()

    // ServiceStart, MetricsSnapshot, ServiceStop should all pass through
    assert(events.exists(_.isInstanceOf[ServiceStart]))
    assert(events.exists(_.isInstanceOf[ServiceStop]))
    assert(events.exists(_.isInstanceOf[MetricsSnapshot]))
    // but debug ReportedEvent should be filtered
    val reported = events.collect { case r: ReportedEvent => r }
    assert(reported.isEmpty)
  }

  // --- noAdhoc ---

  test("5.EventPipe.noAdhoc suppresses adhoc metrics but keeps periodic") {
    val events = service
      .eventStream(agent => agent.adhoc.report *> IO.sleep(2.seconds))
      .map(checkJson)
      .filter(EventPipe.noAdhoc.filter)
      .compile
      .toList
      .unsafeRunSync()

    val snapshots = events.collect { case m: MetricsSnapshot => m }
    // All remaining snapshots should be Periodic, no Adhoc
    assert(snapshots.forall(_.index.isInstanceOf[Periodic]))
    // ServiceStart and ServiceStop should pass through
    assert(events.exists(_.isInstanceOf[ServiceStart]))
    assert(events.exists(_.isInstanceOf[ServiceStop]))
  }

  test("6.EventPipe.noAdhoc keeps periodic metrics") {
    val events = service
      .eventStream(_ => IO.sleep(2.seconds))
      .map(checkJson)
      .filter(EventPipe.noAdhoc.filter)
      .compile
      .toList
      .unsafeRunSync()

    val snapshots = events.collect { case m: MetricsSnapshot => m }
    assert(snapshots.nonEmpty)
    assert(snapshots.forall(_.index.isInstanceOf[Periodic]))
  }

  // --- && with identity ---

  test("7.EventPipe.identity && f is equivalent to f") {
    val events = service
      .eventStream(_ => IO.sleep(4.seconds))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val byFilter = events.filter(EventPipe.indexFilter(2).filter)
    val byComposed = events.filter((EventPipe.identity && EventPipe.indexFilter(2)).filter)
    assert(byFilter == byComposed)
  }

  // --- indexFilter divisor=0 ---

  test("8.EventPipe.indexFilter(0) throws ArithmeticException on periodic metrics") {
    val events = service
      .eventStream(_ => IO.sleep(2.seconds))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    // Applying indexFilter(0) to a periodic MetricsSnapshot should throw ArithmeticException (/ by zero)
    val periodicEvent = events.find {
      case MetricsSnapshot(index, _, _, _) => index.isInstanceOf[Periodic]
      case _                               => false
    }
    assert(periodicEvent.isDefined)
    assertThrows[ArithmeticException] {
      EventPipe.indexFilter(0).filter(periodicEvent.get)
    }
  }

  test("9.EventPipe.indexFilter(0) passes non-periodic events through") {
    val events = service
      .eventStream(agent => agent.adhoc.report)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val startEvent = events.find(_.isInstanceOf[ServiceStart])
    assert(startEvent.isDefined)
    // ServiceStart should pass through even with divisor=0
    assert(EventPipe.indexFilter(0).filter(startEvent.get))
  }

  test("10.EventPipe.indexFilter(0) passes adhoc metrics through") {
    val events = service
      .eventStream(agent => agent.adhoc.report)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val adhocEvent = events.find {
      case MetricsSnapshot(index, _, _, _) => index.isInstanceOf[Adhoc]
      case _                               => false
    }
    assert(adhocEvent.isDefined)
    // Adhoc snapshots should pass through regardless of divisor
    assert(EventPipe.indexFilter(0).filter(adhocEvent.get))
  }

  // --- indexFilter divisor=1 ---

  test("11.EventPipe.indexFilter(1) keeps all periodic metrics") {
    val events = service
      .eventStream(_ => IO.sleep(3.seconds))
      .map(checkJson)
      .filter(EventPipe.indexFilter(1).filter)
      .compile
      .toList
      .unsafeRunSync()

    val allEvents = service
      .eventStream(_ => IO.sleep(3.seconds))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    // indexFilter(1) keeps all because every index % 1 == 0
    val filteredMetrics = events.count(_.isInstanceOf[MetricsSnapshot])
    val allMetrics = allEvents.count(_.isInstanceOf[MetricsSnapshot])
    assert(filteredMetrics == allMetrics)
  }
}
