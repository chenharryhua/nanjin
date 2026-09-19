package mtest.guard

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.{
  MetricsSnapshot,
  ReportedEvent,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import com.github.chenharryhua.nanjin.guard.observers.idempotencyKey
import munit.CatsEffectSuite

import scala.concurrent.duration.DurationInt

class IdempotencyKeyTest extends CatsEffectSuite {

  // A short-lived service that starts, reports metrics once, logs, and stops normally, so the stream carries
  // a representative mix of event types.
  private val events: IO[List[Event]] =
    TaskGuard[IO]("idem")
      .service("idem-key")
      .updateConfig(_.withReportPolicy(_.crontab(_.secondly).repeat))
      .eventStream(_.logger.info("hello"))
      .interruptAfter(3.seconds)
      .compile
      .toList

  test("1.every event yields a non-empty key") {
    events.map { events =>
      assert(events.nonEmpty)
      assert(events.forall(e => idempotencyKey(e).key.nonEmpty))
    }
  }

  test("2.the key is stable: the same event always maps to the same key") {
    events.map { events =>
      assert(events.forall(e => idempotencyKey(e) == idempotencyKey(e)))
    }
  }

  test("3.the key embeds the serviceId so different services never collide") {
    events.map { events =>
      assert(events.forall(e => idempotencyKey(e).key.startsWith(e.serviceIdentity.serviceId.value.toString)))
    }
  }

  test("4.ServiceStart keys carry the start tag and tick index") {
    events.map { events =>
      val starts = events.collect { case e: ServiceStart => e }
      assert(starts.nonEmpty)
      starts.foreach(e =>
        assert(idempotencyKey(e).key == s"${e.serviceIdentity.serviceId.value}-start-${e.tick.index}"))
    }
  }

  test("5.ServiceStop keys carry the stop tag") {
    events.map { events =>
      val stops = events.collect { case e: ServiceStop => e }
      assert(stops.nonEmpty)
      stops.foreach(e => assert(idempotencyKey(e).key == s"${e.serviceIdentity.serviceId.value}-stop"))
    }
  }

  test("6.keys are distinct across events of different kinds within one run") {
    events.map { events =>
      // start, stop, and each periodic metrics snapshot get their own key; no two collapse.
      val keys = events.map(idempotencyKey)
      assert(keys.distinct.size == keys.size)
    }
  }

  test("7.periodic MetricsSnapshot keys carry the metrics-periodic tag and tick index") {
    // a dedicated run with a fast periodic report policy, so the stream carries Periodic snapshots
    TaskGuard[IO]("idem")
      .service("idem-periodic")
      .updateConfig(_.withReportPolicy(_.fixedDelay(100.millis).repeat))
      .eventStream(_ => IO.sleep(250.millis))
      .compile
      .toList
      .map { xs =>
        val periodic =
          xs.collect { case e: MetricsSnapshot if e.index.isInstanceOf[MetricsSnapshot.Periodic] => e }
        assert(periodic.nonEmpty)
        periodic.foreach { e =>
          val tick = e.index.asInstanceOf[MetricsSnapshot.Periodic].tick
          assert(
            idempotencyKey(e).key == s"${e.serviceIdentity.serviceId.value}-metrics-periodic-${tick.index}")
        }
      }
  }

  test("8.ReportedEvent keys carry the reported tag and correlation") {
    // Info logs are filtered by default; lower the threshold so the log becomes a ReportedEvent
    TaskGuard[IO]("idem")
      .service("idem-reported")
      .updateConfig(_.withLogThreshold(_.Info, _.Info))
      .eventStream(_.logger.info("hello"))
      .compile
      .toList
      .map { xs =>
        val reported = xs.collect { case e: ReportedEvent => e }
        assert(reported.nonEmpty)
        reported.foreach(e =>
          assert(
            idempotencyKey(e).key == s"${e.serviceIdentity.serviceId.value}-reported-${e.correlation.value}"))
      }
  }

  test("9.ServicePanic keys carry the panic tag and tick index") {
    // a dedicated run that crashes once then stops, so the stream carries a ServicePanic
    TaskGuard[IO]("idem")
      .service("idem-panic")
      .updateConfig(_.withRestartPolicy(1.hour, _.fixedDelay(100.millis).repeat.limited(1)))
      .eventStream(_ => IO.raiseError(new RuntimeException("boom")))
      .compile
      .toList
      .map { panicEvents =>
        val panics = panicEvents.collect { case e: ServicePanic => e }
        assert(panics.nonEmpty)
        panics.foreach(e =>
          assert(idempotencyKey(e).key == s"${e.serviceIdentity.serviceId.value}-panic-${e.tick.index}"))
      }
  }

  test("10.adhoc MetricsSnapshot keys carry the metrics-adhoc tag and epoch-milli scrape time") {
    // adhoc.report produces an Adhoc-indexed snapshot (the periodic report policy yields Periodic ones)
    TaskGuard[IO]("idem")
      .service("idem-adhoc")
      .eventStream(_.adhoc.report)
      .compile
      .toList
      .map { xs =>
        val adhoc =
          xs.collect { case e: MetricsSnapshot if e.index.isInstanceOf[MetricsSnapshot.Adhoc] => e }
        assert(adhoc.nonEmpty)
        adhoc.foreach { e =>
          val ts = e.index.asInstanceOf[MetricsSnapshot.Adhoc].scrapeTime
          assert(
            idempotencyKey(e).key ==
              s"${e.serviceIdentity.serviceId.value}-metrics-adhoc-${ts.value.toInstant.toEpochMilli}")
        }
      }
  }
}
