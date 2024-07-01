package mtest.guard

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import cats.effect.unsafe.implicits.global
import cats.implicits.{catsSyntaxFlatMapOps, catsSyntaxSemigroup}
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.event.{retrieveDeadlocks, retrieveGauge, retrieveInstrument}
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.generic.JsonCodec
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

@JsonCodec
case class UserGauge(a: Int, b: String)

class GaugeTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("gauge").service("gauge").updateConfig(_.withMetricReport(policies.crontab(_.secondly)))

  test("user gauge") {
    val res: MetricReport = service.eventStream { ga =>
      (ga.jvmGauge.heapMemory |+|
        ga.jvmGauge.deadlocks |+|
        ga.jvmGauge.garbageCollectors |+|
        ga.gauge("user").register(IO(UserGauge(1, "a")))).surround(IO.sleep(4.seconds))
    }.map(checkJson).evalMapFilter(e => IO(metricReport(e))).compile.lastOrError.unsafeRunSync()

    val ug = retrieveGauge[UserGauge](res.snapshot.gauges).values.head
    assert(ug.a == 1)
    assert(ug.b == "a")

    assert(retrieveGauge[Int](res.snapshot.gauges).isEmpty)
    assert(retrieveInstrument(res.snapshot.gauges).size == 2)
    assert(retrieveDeadlocks(res.snapshot.gauges).isEmpty)
  }

  test("2.gauge") {
    val policy = policies.crontab(_.secondly)
    val mr: MetricReport = service
      .updateConfig(_.withJmx(identity))
      .eventStream { agent =>
        val gauge: Resource[IO, Ref[IO, Float]] =
          agent.gauge("free memory").register(IO(Runtime.getRuntime.freeMemory())) >>
            agent.gauge("cost IO").register(IO(1), policy, agent.zoneId) >>
            agent.gauge("cost ByName").register(IO(2), policy, agent.zoneId) >>
            agent.gauge("time").timed >>
            agent.jvmGauge.classloader >>
            agent.jvmGauge.deadlocks >>
            agent.jvmGauge.heapMemory >>
            agent.jvmGauge.nonHeapMemory >>
            agent.jvmGauge.garbageCollectors >>
            agent.jvmGauge.threadState >>
            agent.healthCheck("health check IO").register(IO.raiseError(new Exception)) >>
            agent.healthCheck("health check ByName").register(IO(true)) >>
            agent.healthCheck("cost check IO").register(IO(true), policy, agent.zoneId) >>
            agent.healthCheck("cost check ByName").register(hc = IO(true), policy, agent.zoneId) >>
            Resource.eval(Ref[IO].of(0.1f)).flatMap(ac => agent.gauge("cell").register(ac.get).map(_ => ac))

        gauge.use(box =>
          agent
            .ticks(policies.fixedDelay(1.seconds).limited(3))
            .evalTap(_ => box.updateAndGet(_ + 1))
            .compile
            .drain) >>
          agent.metrics.report
      }
      .map(checkJson)
      .evalTap(console.text[IO])
      .evalMapFilter(e => IO(metricReport(e)))
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(mr.snapshot.gauges.isEmpty)
  }

}
