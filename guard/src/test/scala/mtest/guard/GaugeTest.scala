package mtest.guard

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxFlatMapOps
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.event.retrieveGauge
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.generic.JsonCodec
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

@JsonCodec
case class UserGauge(a: Int, b: String)

class GaugeTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("gauge").service("gauge").updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))

  test("1.user gauge") {
    val res: MetricReport = service.eventStream { ga =>
      ga.facilitator("test")
        .metrics
        .gauge("user")
        .register(IO(UserGauge(1, "a")))
        .surround(IO.sleep(4.seconds))
    }.map(checkJson).evalMapFilter(e => IO(metricReport(e))).compile.lastOrError.unsafeRunSync()

    val ug = retrieveGauge[UserGauge](res.snapshot.gauges).values.head
    assert(ug.a == 1)
    assert(ug.b == "a")
  }

  test("2.gauge") {
    val policy = Policy.crontab(_.secondly)
    val mr: MetricReport = service
      .updateConfig(_.withJmx(identity))
      .eventStream { ag =>
        val mtx = ag.facilitator("agent").metrics
        val gauge: Resource[IO, Ref[IO, Float]] =
          mtx.idleGauge("idle") >>
            mtx.activeGauge("timed") >>
            mtx.gauge("free memory").register(IO(Runtime.getRuntime.freeMemory())) >>
            mtx.gauge("cost IO").register(IO(1), policy, ag.zoneId) >>
            mtx.gauge("cost ByName").register(IO(2), policy, ag.zoneId) >>
            mtx.healthCheck("health check IO").register(IO.raiseError(new Exception)) >>
            mtx.healthCheck("health check ByName").register(IO(true)) >>
            mtx.healthCheck("cost check IO").register(IO(true), policy, ag.zoneId) >>
            mtx.healthCheck("cost check ByName").register(hc = IO(true), policy, ag.zoneId) >>
            Resource.eval(Ref[IO].of(0.1f)).flatMap(ac => mtx.gauge("cell").register(ac.get).map(_ => ac))

        gauge.use(box =>
          ag.ticks(Policy.fixedDelay(1.seconds).limited(3))
            .evalTap(_ => box.updateAndGet(_ + 1))
            .compile
            .drain) >>
          ag.adhoc.report
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
