package mtest.guard

import cats.data.Kleisli
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.implicits.toFunctorFilterOps
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.metrics.MetricID
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.retrieve
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class GaugeTest extends AnyFunSuite {
  private val service =
    TaskGuard[IO]("gauge").service("gauge")

  test("1.gauge") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("gauge")(_.gauge("gauge", _.register(IO(1)))
          .map(_ => Kleisli((_: Unit) => IO.unit)))
        .surround(agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val gauge = retrieve.gauge[Int](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(gauge.values.head == 1)
  }

  test("2.health check") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("health")(
          _.healthCheck(
            "health",
            _.withTimeout(1.second)
              .withPolicy(_.crontab(_.secondly).repeat)
              .enable(true)
              .register(IO(true))))
        .surround(IO.sleep(3.seconds) >> agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val health: Map[MetricID, Boolean] = retrieve.healthCheck(mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(health.values.head)
  }

  test("3.active gauge") {
    val mr = service.eventStream { agent =>
      agent.facilitate("active")(_.activeGauge("active")).surround(agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val active = retrieve.gauge[Json](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(active.values.nonEmpty)
  }

  test("4.idle gauge") {
    val mr = service.eventStream { agent =>
      agent.facilitate("idle")(_.idleGauge("idle")).use(_.wakeUp >> agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val idle = retrieve.gauge[Json](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(idle.values.nonEmpty)
  }

  test("5.no policy gauge should recompute on each scrape") {
    val snapshots = service.eventStream { agent =>
      val setup = for {
        ref <- Resource.eval(cats.effect.Ref[IO].of(0))
        _ <- agent.facilitate("counter")(_.gauge("counter", _.register(ref.modify(i => (i + 1, i + 1)))))
      } yield ()

      setup.surround(agent.adhoc.report >> agent.adhoc.report)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).take(2).compile.toList.unsafeRunSync()

    assert(snapshots.size == 2)
    val first = retrieve.gauge[Int](snapshots.head.snapshot.gauges).values.head
    val second = retrieve.gauge[Int](snapshots(1).snapshot.gauges).values.head
    assert(first == 1)
    assert(second == 2)
  }

  test("6.gauge timeout") {
    val mr = service.eventStream { agent =>
      agent.facilitate("timeout.gauge")(
        _.gauge(
          "gauge",
          _.withTimeout(1.second).enable(true)
            .register(IO.never[Int]))
          .surround(agent.adhoc.report.void))
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val gauge = retrieve.gauge[Int](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(gauge.isEmpty)
  }

  test("7.gauge exception") {
    val mr = service.eventStream { agent =>
      agent.facilitate("timeout.gauge")(
        _.gauge(
          "gauge",
          _.withTimeout(1.second).enable(true)
            .register(IO.raiseError[Int](new Exception("oops"))))
          .surround(agent.adhoc.report.void))
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val gauge = retrieve.gauge[Int](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(gauge.isEmpty)
  }

  test("8.expensive") {
    def compute(fd: FiniteDuration) =
      for {
        s <- IO.realTimeInstant
        _ <- IO.sleep(fd)
        e <- IO.realTimeInstant
      } yield (fd.toSeconds, s, e)

    service.eventStream { agent =>
      agent.facilitate("expensive") { fac =>
        val mtx = for {
          _ <- fac.gauge("3", _.withPolicy(_.fixedDelay(15.minutes).repeat).register(compute(3.second)))
          _ <- fac.gauge("2", _.withPolicy(_.fixedDelay(15.minutes).repeat).register(compute(2.second)))
          _ <- fac.gauge("1", _.withPolicy(_.fixedDelay(15.minutes).repeat).register(compute(1.second)))
        } yield ()
        mtx.surround(agent.adhoc.report.void)
      }
    }.compile.drain.unsafeRunSync()
  }

  test("9.policy gauge should return cached value between refresh ticks") {
    val snapshots = service.eventStream { agent =>
      val setup = for {
        ref <- Resource.eval(cats.effect.Ref[IO].of(0))
        _ <- agent.facilitate("cached-counter")(
          _.gauge(
            "counter",
            _.withPolicy(_.fixedDelay(24.hours).repeat)
              .register(ref.modify(i => (i + 1, i + 1)))))
      } yield ()

      setup.surround(agent.adhoc.report >> agent.adhoc.report)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).take(2).compile.toList.unsafeRunSync()

    assert(snapshots.size == 2)
    val first = retrieve.gauge[Int](snapshots.head.snapshot.gauges).values.head
    val second = retrieve.gauge[Int](snapshots(1).snapshot.gauges).values.head
    assert(first == 1)
    assert(second == 1)
  }

  test("10.ratio gauge is collected as ratio data") {
    val snapshots = service.eventStream { agent =>
      agent.facilitate("ratio") { fac =>
        fac.ratio("ratio").use { ratio =>
          ratio.incBoth(1, 2) >> agent.adhoc.report.void
        }
      }
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).take(1).compile.toList.unsafeRunSync()

    assert(snapshots.nonEmpty)
    val ratios = retrieve.ratio(snapshots.head.snapshot.gauges)
    assert(ratios.nonEmpty)
    assert(ratios.values.head.asString.exists(_.contains("50")))
  }

  test("12.frequency counter - accumulates tags") {
    val snapshots = service.eventStream { agent =>
      agent.facilitate("freq") { fac =>
        fac.frequencyCounter("errors").use { fc =>
          fc.inc("getCustomer") >>
            fc.inc("createOrder") >>
            fc.inc("getCustomer") >>
            fc.inc("getCustomer", 5) >>
            agent.adhoc.report.void
        }
      }
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).take(1).compile.toList.unsafeRunSync()

    assert(snapshots.nonEmpty)
    val gauges = retrieve.gauge[Map[String, Long]](snapshots.head.snapshot.gauges)
    assert(gauges.nonEmpty)
    val counts = gauges.values.head
    assert(counts("getCustomer") == 7)
    assert(counts("createOrder") == 1)
  }

  test("13.frequency counter - policy reset clears counts") {
    val snapshots = service.eventStream { agent =>
      agent.facilitate("freq-reset") { fac =>
        fac.frequencyCounter("errors", _.withPolicy(_.fixedDelay(200.millis).repeat)).use { fc =>
          fc.inc("a", 10) >>
            IO.sleep(500.millis) >>
            agent.adhoc.report.void
        }
      }
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).take(1).compile.toList.unsafeRunSync()

    assert(snapshots.nonEmpty)
    // After 500ms with 200ms reset policy, map is empty -> gauge reports Json.Null -> filtered from snapshot
    val gauges = retrieve.gauge[Map[String, Long]](snapshots.head.snapshot.gauges)
    assert(gauges.isEmpty)
  }

  test("14.frequency counter - disabled is noop") {
    val snapshots = service.eventStream { agent =>
      agent.facilitate("freq-disabled") { fac =>
        fac.frequencyCounter("errors", _.enable(false)).use { fc =>
          fc.inc("x") >>
            fc.inc("y") >>
            agent.adhoc.report.void
        }
      }
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).take(1).compile.toList.unsafeRunSync()

    assert(snapshots.nonEmpty)
    // No gauge registered for disabled frequency counter
    val gauges = retrieve.gauge[Map[String, Long]](snapshots.head.snapshot.gauges)
    assert(gauges.isEmpty)
  }

  test("15.gauge returning Json.Null is excluded from snapshot") {
    val snapshots = service.eventStream { agent =>
      agent.facilitate("null-gauge") { fac =>
        for {
          _ <- fac.gauge("always-null", _.register(IO.pure(Json.Null)))
          _ <- fac.gauge("has-value", _.register(IO.pure(Json.fromInt(42))))
        } yield ()
      }.surround(agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).take(1).compile.toList.unsafeRunSync()

    assert(snapshots.nonEmpty)
    val gauges = retrieve.gauge[Json](snapshots.head.snapshot.gauges)
    // "always-null" is filtered out, only "has-value" appears
    assert(gauges.size == 1)
    assert(gauges.values.head == Json.fromInt(42))
  }
}
