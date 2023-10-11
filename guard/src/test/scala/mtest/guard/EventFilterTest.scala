package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.{crontabs, policies}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{MetricReport, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.eventFilters
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class EventFilterTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] = TaskGuard[IO]("event.filters").service("filters")

  test("1.isPivotalEvent") {
    val List(a, b) = service.eventStream { agent =>
      agent.action("pivotal", _.bipartite).retry(IO(())).run
    }.filter(eventFilters.isPivotalEvent).compile.toList.unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceStop])
  }

  test("2.isServiceEvent") {
    val List(a, b) = service.eventStream { agent =>
      agent.action("service", _.bipartite).retry(IO(())).run
    }.filter(eventFilters.isServiceEvent).compile.toList.unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceStop])
  }

  test("3.nonSuppress") {
    val List(a, b) = service.eventStream { agent =>
      agent.action("nonSuppress", _.bipartite.suppressed).retry(IO(())).run
    }.filter(eventFilters.nonSuppress).compile.toList.unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceStop])
  }

  test("4.sampling - FiniteDuration") {
    val List(a, b, c, d, e) = service
      .updateConfig(_.withMetricReport(policies.crontab(_.secondly)))
      .eventStream(agent => agent.action("sleep").retry(IO.sleep(5.seconds)).run >> agent.metrics.report)
      .filter(eventFilters.sampling(3.seconds))
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[MetricReport])
    assert(c.isInstanceOf[MetricReport])
    assert(d.isInstanceOf[MetricReport])
    assert(e.isInstanceOf[ServiceStop])
  }

  test("5.sampling - divisor") {
    val List(a, b, c, d) = service
      .updateConfig(_.withMetricReport(policies.crontab(_.secondly)))
      .eventStream(agent => agent.action("sleep").retry(IO.sleep(5.seconds)).run >> agent.metrics.report)
      .filter(eventFilters.sampling(3))
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[MetricReport])
    assert(c.isInstanceOf[MetricReport])
    assert(d.isInstanceOf[ServiceStop])
  }

  test("6.sampling - cron") {
    val policy = policies.crontab(_.secondly)
    val lst = service
      .updateConfig(_.withMetricReport(policy))
      .eventStream(agent => agent.action("sleep").retry(IO.sleep(7.seconds)).run >> agent.metrics.report)
      .debug()
      .filter(eventFilters.sampling(crontabs.every3Seconds))
      .compile
      .toList
      .unsafeRunSync()
    assert(lst.size < 6)
  }
}
