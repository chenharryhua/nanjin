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

  test("1.counter") {
    task
      .service("counter")
      .eventStream { agent =>
        agent
          .facilitate("ga") { fac =>
            fac.metrics.counter("counter").evalMap { c =>
              c.inc(2) >> agent.adhoc.report
            }
          }
          .use_
      }
      .map(checkJson)
      .mapFilter(metricReport)
      .compile
      .last
      .unsafeRunSync()
      .get
  }

  test("2.meter") {
    val mr = task
      .service("meter")
      .eventStream { agent =>
        agent
          .facilitate("ga") { fac =>
            val ga = fac.metrics
            ga.meter("counter").evalMap { meter =>
              meter.update(3)
            }
          }
          .use(_ => agent.adhoc.report)
      }
      .map(checkJson)
      .mapFilter(metricReport)
      .compile
      .last
      .unsafeRunSync()
      .get
    assert(meter(mr) == 3)
  }

  test("3.histogram") {
    val mr = task
      .service("histogram")
      .eventStream { agent =>
        agent
          .facilitate("ga") {
            _.metrics.histogram("histogram", _.withReservoir(new SlidingWindowReservoir(5))).evalMap {
              histo =>
                histo.update(200)
            }
          }
          .use(_ => agent.adhoc.report)
      }
      .map(checkJson)
      .mapFilter(metricReport)
      .compile
      .last
      .unsafeRunSync()
      .get

    assert(histogram(mr) == 1)
  }

  test("4.health-check") {
    val res = task
      .service("heal-check")
      .eventStream { agent =>
        agent.facilitate("ga")(_.metrics.healthCheck("c1").register(IO(true))).use(_ => agent.adhoc.report)
      }
      .map(checkJson)
      .mapFilter(metricReport)
      .compile
      .lastOrError
      .unsafeRunSync()

    val hc = retrieveHealthChecks(res.snapshot.gauges)
    assert(hc.size == 1)
    assert(hc.values.forall(identity))
  }

  test("5.timer") {
    val mr = task
      .service("timer")
      .eventStream { ga =>
        ga.facilitate("timer")(
          _.metrics.timer("timer", _.withReservoir(new SlidingWindowReservoir(10))).evalMap { timer =>
            timer.update(2.minutes) >>
              timer.update(5.minutes)
          })
          .surround(ga.adhoc.report)
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
