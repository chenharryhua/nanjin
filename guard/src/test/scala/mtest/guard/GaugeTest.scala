package mtest.guard

import cats.data.Kleisli
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.implicits.toFunctorFilterOps
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.{retrieveGauge, retrieveHealthChecks, Event, MetricID}
import io.circe.Json
import io.github.timwspence.cats.stm.STM
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class GaugeTest extends AnyFunSuite {
  private val service =
    TaskGuard[IO]("gauge").service("gauge").updateConfig(_.withLogFormat(_.Console_PlainText))

  test("1.gauge") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("gauge")(_.gauge("gauge", _.register(IO(1)))
          .map(_ => Kleisli((_: Unit) => IO.unit)))
        .surround(agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val gauge = retrieveGauge[Int](mr.snapshot.gauges)
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
              .withPolicy(_.crontab(_.secondly))
              .enable(true)
              .register(IO(true))))
        .surround(IO.sleep(3.seconds) >> agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val health: Map[MetricID, Boolean] = retrieveHealthChecks(mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(health.values.head)
  }

  test("3.active gauge") {
    val mr = service.eventStream { agent =>
      agent.facilitate("active")(_.activeGauge("active")).surround(agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val active = retrieveGauge[Json](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(active.values.nonEmpty)
  }

  test("4.idle gauge") {
    val mr = service.eventStream { agent =>
      agent.facilitate("idle")(_.idleGauge("idle")).use(_.wakeUp >> agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val idle = retrieveGauge[Json](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(idle.values.nonEmpty)
  }

  test("5.permanent counter") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("permanent")(_.permanentCounter("permanent"))
        .use(_.inc(1999) >> agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val permanent = retrieveGauge[Json](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(permanent.values.head.as[Int].toOption.get == 1999)
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
    val gauge = retrieveGauge[Int](mr.snapshot.gauges)
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
    val gauge = retrieveGauge[Int](mr.snapshot.gauges)
    assert(mr.snapshot.nonEmpty)
    assert(gauge.isEmpty)
  }

  test("8.expensive") {
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
          _ <- fac.gauge("3", _.withPolicy(_.fixedDelay(15.minutes)).register(compute(3.second)))
          _ <- fac.gauge("2", _.withPolicy(_.fixedDelay(15.minutes)).register(compute(2.second)))
          _ <- fac.gauge("1", _.withPolicy(_.fixedDelay(15.minutes)).register(compute(1.second)))
        } yield ()
        mtx.surround(agent.adhoc.report.void)
      }
    }.debug().compile.drain.unsafeRunSync()
  }

  test("transactional gauge") {
    service.eventStream { agent =>
      val mtx =
        agent.facilitate("transactional") { fac =>
          for {
            stm <- Resource.eval(STM.runtime[IO])
            a <- fac.txnGauge(stm, 10)("account-a")
            b <- fac.txnGauge(stm, 10)("account-b")
          } yield { (n: Int) =>
            val transfer = for {
              balance <- a.get
              _ <- stm.check(balance > n)
              _ <- a.modify(_ - n)
              _ <- b.modify(_ + n)
            } yield ()
            stm.commit(transfer)
          }
        }

      mtx.use(_(5) >> agent.adhoc.report.void)
    }.compile.drain.unsafeRunSync()

  }
}
