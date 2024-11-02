package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.{
  retrieveCounter,
  retrieveHistogram,
  retrieveMeter,
  retrieveRiskCounter,
  retrieveTimer,
  MeasurementUnit
}
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.generic.JsonCodec
import org.scalatest.funsuite.AnyFunSuite

import java.time.{ZoneId, ZonedDateTime}

@JsonCodec
final case class SystemInfo(now: ZonedDateTime, on: Boolean, size: Int)

class MetricsTest extends AnyFunSuite {
  val zoneId: ZoneId = ZoneId.systemDefault()

  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("metrics")
      .updateConfig(_.withZoneId(zoneId).withHostName(HostName.local_host).disableHttpServer.disableJmx)
      .service("metrics")

  test("1.counter") {
    val mr = service.eventStream { agent =>
      agent
        .metrics("counter")(_.counter("counter").map(_.kleisli[Long](identity)))
        .use(_.run(10) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()
    assert(retrieveCounter(mr.snapshot.counters).values.head == 10)
    assert(retrieveRiskCounter(mr.snapshot.counters).values.isEmpty)
  }

  test("2.counter risk") {
    val mr = service.eventStream { agent =>
      agent
        .metrics("counter")(_.counter("counter", _.asRisk).map(_.kleisli))
        .use(_.run(10) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()
    assert(retrieveRiskCounter(mr.snapshot.counters).values.head == 10)
    assert(retrieveCounter(mr.snapshot.counters).values.isEmpty)
  }

  test("3.counter disable") {
    val mr = service.eventStream { agent =>
      agent.facilitate("counter")(_.counter("counter", _.enable(false))).use(_.run(10) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()
    assert(retrieveCounter(mr.snapshot.counters).values.isEmpty)
    assert(retrieveRiskCounter(mr.snapshot.counters).values.isEmpty)
  }

  test("4.meter") {
    val mr = service.eventStream { agent =>
      agent
        .metrics("meter")(_.meter("meter", _.withUnit(_.BYTES)).map(_.kleisli[Long](identity)))
        .use(_.run(10) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()
    val meter = retrieveMeter(mr.snapshot.meters).values.head
    assert(meter.sum == 10)
    assert(meter.unit == MeasurementUnit.NJInformationUnit.BYTES)
  }

  test("5.meter disable") {
    val mr = service.eventStream { agent =>
      agent
        .metrics("meter")(_.meter("meter", _.enable(false)).map(_.kleisli))
        .use(_.run(10) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()
    assert(retrieveMeter(mr.snapshot.meters).isEmpty)
  }

  test("6.histogram") {
    val mr = service.eventStream { agent =>
      agent
        .metrics("histogram")(_.histogram("histogram", _.withUnit(_.BYTES)).map(_.kleisli[Long](identity)))
        .use(_.run(10) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()
    val histo = retrieveHistogram(mr.snapshot.histograms).values.head
    assert(histo.updates == 1)
    assert(histo.unit == MeasurementUnit.NJInformationUnit.BYTES)
  }

  test("7.histogram disable") {
    val mr = service.eventStream { agent =>
      agent
        .metrics("histogram")(_.histogram("histogram", _.enable(false)).map(_.kleisli))
        .use(_.run(10) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()
    assert(retrieveHistogram(mr.snapshot.histograms).isEmpty)
  }

  test("8.timer") {
    val mr = service.eventStream { agent =>
      agent
        .metrics("timer")(_.timer("timer").map(_.kleisli[Long](identity)))
        .use(_.run(10) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()
    val timer = retrieveTimer(mr.snapshot.timers).values.head
    assert(timer.calls == 1)
  }

  test("9.timer disable") {
    val mr = service.eventStream { agent =>
      agent
        .metrics("timer")(_.timer("timer", _.enable(false)).map(_.kleisli))
        .use(_.run(10) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()
    assert(retrieveTimer(mr.snapshot.timers).isEmpty)
  }
}
