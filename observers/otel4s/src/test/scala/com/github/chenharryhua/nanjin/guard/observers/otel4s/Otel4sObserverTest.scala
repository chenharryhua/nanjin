package com.github.chenharryhua.nanjin.guard.observers.otel4s

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.*
import io.opentelemetry.sdk.logs.data.LogRecordData
import munit.CatsEffectSuite
import org.typelevel.otel4s.logs.Severity
import org.typelevel.otel4s.oteljava.testkit.logs.{LogRecordExpectation, LogRecordExpectations, LogsTestkit}

import scala.concurrent.duration.*

/** Tests for `Otel4sObserver`, driving it against the in-memory `LogsTestkit` so the emitted OpenTelemetry
  * log records (body and severity) can be asserted.
  */
class Otel4sObserverTest extends CatsEffectSuite {

  private val service = TaskGuard[IO]("otel4s-test")
    .service("otel4s-observer-test")
    .updateConfig(
      _.withLogThreshold(_.Info, _.Info)
        .withRestartPolicy(1.hour, _.fixedDelay(100.millis).repeat.limited(1)))

  /** Run the given service actions through the observer and return both the events that flowed through the
    * pipe and the log records the observer emitted to OpenTelemetry.
    */
  private def run(actions: com.github.chenharryhua.nanjin.guard.service.Agent[IO] => IO[Unit])
    : IO[(List[Event], List[LogRecordData])] =
    LogsTestkit
      .inMemory[IO]()
      .use { testkit =>
        val observer = Otel4sObserver(testkit.loggerProvider)
        for {
          events <- service.eventStream(actions).through(
            observer.observe("otel4s-observer-test")).compile.toList
          records <- testkit.finishedLogs
        } yield (events, records)
      }

  private def assertLogs(records: List[LogRecordData], expected: LogRecordExpectation*): Unit =
    LogRecordExpectations.checkAll(records, expected*) match {
      case Right(_)         => ()
      case Left(mismatches) => fail(LogRecordExpectations.format(mismatches))
    }

  test("1.emits one log record per event, each passing through unchanged") {
    run(_ => IO.unit).map { case (events, records) =>
      assert(events.exists(_.isInstanceOf[ServiceStart]))
      assert(events.exists(_.isInstanceOf[ServiceStop]))
      // every event that translates is emitted as a record; pass-through preserves all events
      assert(records.size == events.size)
    }
  }

  test("2.each record body is the translated event JSON string") {
    run(_ => IO.unit).map { case (_, records) =>
      // the observer emits AnyValue.string(json.noSpaces); bodies must be non-empty JSON objects
      val bodies = records.flatMap(r => Option(r.getBodyValue).map(_.asString()))
      assert(bodies.nonEmpty)
      assert(bodies.forall(b => b.startsWith("{") && b.endsWith("}")))
    }
  }

  test("3.an info log event maps to Severity.info") {
    run(_.logger.info("hello-info")).map { case (_, records) =>
      val infoRecord = records.find(r => Option(r.getBodyValue).exists(_.asString().contains("hello-info")))
      assert(infoRecord.nonEmpty)
      assertLogs(infoRecord.toList, LogRecordExpectation.any.severity(Severity.info))
    }
  }

  test("4.an error log event maps to Severity.error and carries the message") {
    run(_.logger.error("hello-error")).map { case (_, records) =>
      val errorRecord = records.find(r => Option(r.getBodyValue).exists(_.asString().contains("hello-error")))
      assert(errorRecord.nonEmpty)
      assertLogs(errorRecord.toList, LogRecordExpectation.any.severity(Severity.error))
    }
  }

  test("5.a warn log event maps to Severity.warn") {
    run(_.logger.warn("hello-warn")).map { case (_, records) =>
      val warnRecord = records.find(r => Option(r.getBodyValue).exists(_.asString().contains("hello-warn")))
      assert(warnRecord.nonEmpty)
      assertLogs(warnRecord.toList, LogRecordExpectation.any.severity(Severity.warn))
    }
  }

  test("6.severityText mirrors the severity name") {
    run(_.logger.error("boom-text")).map { case (_, records) =>
      val errorRecord =
        records.find(r => Option(r.getBodyValue).exists(_.asString().contains("boom-text"))).get
      assert(Option(errorRecord.getSeverityText).contains(Severity.error.toString))
    }
  }

  test("7.withTranslator can skip event types") {
    LogsTestkit
      .inMemory[IO]()
      .use { testkit =>
        val observer = Otel4sObserver(testkit.loggerProvider).withTranslator(_.skipMetricsSnapshot)
        for {
          events <- service
            .eventStream(_.adhoc.report)
            .through(observer.observe("otel4s-observer-test"))
            .compile
            .toList
          records <- testkit.finishedLogs
        } yield (events, records)
      }
      .map { case (events, records) =>
        assert(events.exists(_.isInstanceOf[MetricsSnapshot]))
        // the snapshot event still flows through the pipe but is not emitted as a record
        assert(records.size < events.size)
      }
  }
}
