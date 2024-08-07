package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.Policy
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
      agent.action("pivotal", _.bipartite).retry(IO(())).buildWith(identity).use(_.run(()))
    }.map(checkJson).filter(eventFilters.isPivotalEvent).compile.toList.unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceStop])
  }

  test("2.isServiceEvent") {
    val List(a, b) = service.eventStream { agent =>
      agent.action("service", _.bipartite).retry(IO(())).buildWith(identity).use(_.run(()))
    }.map(checkJson).filter(eventFilters.isServiceEvent).compile.toList.unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceStop])
  }

  test("3.nonSuppress") {
    val List(a, b) = service.eventStream { agent =>
      agent.action("nonSuppress", _.bipartite.suppressed).retry(IO(())).buildWith(identity).use(_.run(()))
    }.map(checkJson).filter(eventFilters.nonSuppress).compile.toList.unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceStop])
  }

  test("4.sampling - FiniteDuration") {
    val List(a, b, c, d, e) = service
      .updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))
      .eventStream(agent =>
        agent
          .action("sleep")
          .retry(IO.sleep(5.seconds))
          .buildWith(identity)
          .use(_.run(())) >> agent.metrics.report)
      .map(checkJson)
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
      .updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))
      .eventStream(agent =>
        agent
          .action("sleep")
          .retry(IO.sleep(5.seconds))
          .buildWith(identity)
          .use(_.run(())) >> agent.metrics.report)
      .map(checkJson)
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
    val policy = Policy.crontab(_.secondly)
    val lst = service
      .updateConfig(_.withMetricReport(policy))
      .eventStream(agent =>
        agent
          .action("sleep")
          .retry(IO.sleep(7.seconds))
          .buildWith(identity)
          .use(_.run(())) >> agent.metrics.report)
      .map(checkJson)
      .filter(eventFilters.sampling(_.every3Seconds))
      .compile
      .toList
      .unsafeRunSync()
    assert(lst.size < 6)
  }
}
