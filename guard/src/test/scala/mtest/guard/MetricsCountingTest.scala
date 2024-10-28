package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.codahale.metrics.SlidingWindowReservoir
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.event.{
  retrieveHealthChecks,
  retrieveHistogram,
  retrieveMeter,
  retrieveTimer
}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class MetricsCountingTest extends AnyFunSuite {
  private val task: TaskGuard[IO] =
    TaskGuard[IO]("metrics.counting").updateConfig(_.withZoneId(sydneyTime))

  def meter(mr: MetricReport): Long =
    retrieveMeter(mr.snapshot.meters).values.map(_.sum).sum

  def histogram(mr: MetricReport): Long =
    retrieveHistogram(mr.snapshot.histograms).values.map(_.updates).sum

  test("1.alert") {
    task
      .service("alert")
      .eventStream { agent =>
        val alert = agent.facilitator("msg").messenger
        alert.warn("m1") >>
          alert.info("m2") >>
          alert.error("m3") >>
          alert.done("m4")
      }
      .map(checkJson)
      .compile
      .last
      .unsafeRunSync()
      .get
  }

  test("2.counter") {
    task
      .service("counter")
      .eventStream { agent =>
        val ga = agent.facilitator("ga").metrics
        ga.counter("counter").use { c =>
          c.inc(2) >> agent.adhoc.report
        }
      }
      .map(checkJson)
      .mapFilter(metricReport)
      .compile
      .last
      .unsafeRunSync()
      .get
  }

  test("3.meter") {
    val mr = task
      .service("meter")
      .eventStream { agent =>
        val ga = agent.facilitator("ga").metrics
        ga.meter("counter").use { meter =>
          meter.update(3) >> agent.adhoc.report
        }
      }
      .map(checkJson)
      .mapFilter(metricReport)
      .compile
      .last
      .unsafeRunSync()
      .get
    assert(meter(mr) == 3)
  }

  test("4.histogram") {
    val mr = task
      .service("histogram")
      .eventStream { agent =>
        val ga = agent.facilitator("ga").metrics
        ga.histogram("histogram", _.withReservoir(new SlidingWindowReservoir(5))).use { histo =>
          histo.update(200) >> agent.adhoc.report
        }
      }
      .map(checkJson)
      .mapFilter(metricReport)
      .compile
      .last
      .unsafeRunSync()
      .get

    assert(histogram(mr) == 1)
  }

  test("5.gauge") {
    task
      .service("gauge")
      .eventStream { agent =>
        val g2 = agent.facilitator("a").metrics.gauge("exception").register(IO.raiseError[Int](new Exception))
        val g3 = agent.facilitator("a").metrics.gauge("good").register(IO(10))
        g2.both(g3).surround(IO.sleep(3.seconds) >> agent.adhoc.report)
      }
      .map(checkJson)
      .map {
        case event: MetricReport => assert(event.snapshot.gauges.size == 2)
        case _                   => ()
      }
      .compile
      .drain
      .unsafeRunSync()
  }

  test("6.health-check") {
    val res = task
      .service("heal-check")
      .eventStream { agent =>
        val ga = agent.facilitator("ga").metrics
        (ga.healthCheck("c1").register(IO(true)) >> ga.healthCheck("c2").register(IO(true))).use(_ =>
          agent.adhoc.report)
      }
      .map(checkJson)
      .mapFilter(metricReport)
      .compile
      .lastOrError
      .unsafeRunSync()

    val hc = retrieveHealthChecks(res.snapshot.gauges)
    assert(hc.size == 2)
    assert(hc.values.forall(identity))
  }

  test("7.timer") {
    val mr = task
      .service("timer")
      .eventStream { ga =>
        ga.facilitator("timer").metrics.timer("timer", _.withReservoir(new SlidingWindowReservoir(10))).use {
          timer =>
            timer.update(2.minutes) >>
              timer.update(5.minutes) >>
              ga.adhoc.report
        }
      }
      .map(checkJson)
      .mapFilter(metricReport)
      .compile
      .last
      .unsafeRunSync()
      .get

    assert(retrieveTimer(mr.snapshot.timers).values.map(_.calls).sum == 2)
  }
}
