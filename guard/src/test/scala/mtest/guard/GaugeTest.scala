package mtest.guard

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toFunctorFilterOps
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.MetricID
import com.github.chenharryhua.nanjin.guard.event.{eventFilters, retrieveGauge, retrieveHealthChecks}
import com.github.chenharryhua.nanjin.guard.observers.console
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class GaugeTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("gauge").service("gauge")

  test("1.gauge") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("gauge")(_.gauge("gauge").register(IO(1)).map(_ => Kleisli((_: Unit) => IO.unit)))
        .surround(agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    val gauge = retrieveGauge[Int](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(gauge.values.head == 1)
  }

  test("2.health check") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("health")(
          _.healthCheck("health", _.withTimeout(1.second).enable(true)).register(IO(true)))
        .surround(agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    val health: Map[MetricID, Boolean] = retrieveHealthChecks(mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(health.values.head)
  }

  test("3.active gauge") {
    val mr = service.eventStream { agent =>
      agent.facilitate("active")(_.activeGauge("active", _.enable(true))).surround(agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    val active = retrieveGauge[Json](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(active.values.nonEmpty)
  }

  test("4.idle gauge") {
    val mr = service.eventStream { agent =>
      agent.facilitate("idle")(_.idleGauge("idle", _.enable(true))).use(_.run(()) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    val idle = retrieveGauge[Json](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(idle.values.nonEmpty)
  }

  test("5.permanent counter") {
    val mr = service.eventStream { agent =>
      agent.facilitate("permanent")(_.permanentCounter("permanent")).use(_.run(1999) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    val permanent = retrieveGauge[Json](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(permanent.values.head.as[String].toOption.get == "1,999")
  }

  test("6.gauge timeout") {
    val mr = service.eventStream { agent =>
      agent.facilitate("timeout.gauge")(
        _.gauge("gauge", _.withTimeout(1.second).enable(true))
          .register(IO.never[Int])
          .surround(agent.adhoc.report))
    }.map(checkJson)
      .evalTap(console.text[IO])
      .mapFilter(eventFilters.metricReport)
      .compile
      .lastOrError
      .unsafeRunSync()
    val gauge = retrieveGauge[Int](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(gauge.isEmpty)
  }

  test("7.gauge exception") {
    val mr = service.eventStream { agent =>
      agent.facilitate("timeout.gauge")(
        _.gauge("gauge", _.withTimeout(1.second).enable(true))
          .register(IO.raiseError[Int](new Exception("oops")))
          .surround(agent.adhoc.report))
    }.map(checkJson)
      .evalTap(console.text[IO])
      .mapFilter(eventFilters.metricReport)
      .compile
      .lastOrError
      .unsafeRunSync()
    val gauge = retrieveGauge[Int](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(gauge.isEmpty)
  }

  test("expensive") {
    def compute(fd: FiniteDuration) =
      for {
        s <- IO.realTimeInstant.flatTap(IO.println)
        _ <- IO.sleep(fd)
        e <- IO.realTimeInstant.flatTap(IO.println)
        _ <- IO.println(s"--${fd.toSeconds}--")
      } yield (fd.toSeconds, s, e)

    service.eventStream { agent =>
      agent.facilitate("expensive") { fac =>
        val mtx = for {
          _ <- fac.gauge("3").register(compute(3.second))
          _ <- fac.gauge("2").register(compute(2.second))
          _ <- fac.gauge("1").register(compute(1.second))
        } yield ()
        mtx.surround(agent.adhoc.report)
      }
    }.evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }
}
