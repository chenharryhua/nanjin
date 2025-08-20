package mtest.guard

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.codahale.metrics.SlidingWindowReservoir
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.MetricID
import com.github.chenharryhua.nanjin.guard.event.{
  eventFilters,
  retrieveCounter,
  retrieveHistogram,
  retrieveMeter,
  retrieveRiskCounter,
  retrieveTimer
}
import com.github.chenharryhua.nanjin.guard.metrics.Meter
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.generic.JsonCodec
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.InformationConversions
import squants.information.{Bytes, Information}
import squants.market.MoneyConversions.MoneyConversions
import squants.market.{AUD, Money}
import squants.time.{Milliseconds, Time}
import squants.{Dimensionless, Percent}

import java.time.{ZoneId, ZonedDateTime}
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

@JsonCodec
final case class SystemInfo(now: ZonedDateTime, on: Boolean, size: Int)

class MetricsTest extends AnyFunSuite {
  val zoneId: ZoneId = ZoneId.systemDefault()

  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("metrics")
      .updateConfig(_.withZoneId(zoneId).disableHttpServer.disableJmx)
      .service("metrics")

  test("1.counter") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("counter")(_.counter("counter").map(_.local[Long](identity)))
        .use(_.run(10) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    assert(mr.snapshot.nonEmpty)
    assert(retrieveCounter(mr.snapshot.counters).values.head == 10)
    assert(retrieveRiskCounter(mr.snapshot.counters).values.isEmpty)
  }

  test("2.counter risk") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("counter")(_.counter("counter", _.asRisk).map(_.kleisli))
        .use(_.run(10) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    assert(retrieveRiskCounter(mr.snapshot.counters).values.head == 10)
    assert(retrieveCounter(mr.snapshot.counters).values.isEmpty)
  }

  test("3.counter disable") {
    val mr = service.eventStream { agent =>
      agent.facilitate("counter")(_.counter("counter", _.enable(false))).use(_.run(10) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    assert(mr.snapshot.isEmpty)
    assert(retrieveCounter(mr.snapshot.counters).values.isEmpty)
    assert(retrieveRiskCounter(mr.snapshot.counters).values.isEmpty)
  }

  test("4.meter") {
    val mr = service.eventStream { agent =>
      val meter: Resource[IO, Meter[IO, Money]] = agent.facilitate("meter")(_.meter(AUD)("meter"))
      meter.use(m => m.run(10.AUD) >> m.mark(20) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    val meter = retrieveMeter(mr.snapshot.meters).values.head
    assert(mr.snapshot.nonEmpty)
    assert(meter.aggregate == 30)
    assert(meter.squants.unitSymbol == AUD.symbol)
    assert(meter.squants.dimensionName == Money.name)
  }

  test("5.meter disable") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("meter")(_.meter(Bytes)("meter", _.enable(false)))
        .use(_.run(10.bytes) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    assert(mr.snapshot.isEmpty)
    assert(retrieveMeter(mr.snapshot.meters).isEmpty)
  }

  test("6.histogram") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("histogram")(_.histogram(Bytes)("histogram"))
        .use(m => m.run(10.bytes) >> m.update(20) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    val histo = retrieveHistogram(mr.snapshot.histograms).values.head
    assert(mr.snapshot.nonEmpty)
    assert(histo.updates == 2)
    assert(histo.max == 20)
    assert(histo.squants.unitSymbol == Bytes.symbol)
    assert(histo.squants.dimensionName == Information.name)
  }

  test("6.histogram timer") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("histogram")(_.histogram(Milliseconds)("histogram"))
        .use(m => m.update(1030) >> m.update(200) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    val histo = retrieveHistogram(mr.snapshot.histograms).values.head
    assert(mr.snapshot.nonEmpty)
    assert(histo.updates == 2)
    assert(histo.max == 1030)
    assert(histo.squants.unitSymbol == Milliseconds.symbol)
    assert(histo.squants.dimensionName == Time.name)
  }

  test("6.histogram percent") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("histogram")(_.histogram(Percent)("histogram"))
        .use(m => m.update(30) >> m.update(50) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    val histo = retrieveHistogram(mr.snapshot.histograms).values.head
    assert(mr.snapshot.nonEmpty)
    assert(histo.updates == 2)
    assert(histo.max == 50)
    assert(histo.squants.unitSymbol == Percent.symbol)
    assert(histo.squants.dimensionName == Dimensionless.name)
  }

  test("7.histogram disable") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("histogram")(_.histogram(Bytes)("histogram", _.enable(false)).map(_.kleisli))
        .use(_.run(10.bytes) >>
          agent.adhoc.getSnapshot.map(ss => assert(ss.isEmpty)) >>
          agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    assert(mr.snapshot.isEmpty)
    assert(retrieveHistogram(mr.snapshot.histograms).isEmpty)
  }

  test("8.timer") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("timer")(_.timer("timer").map(_.local[Long](identity)))
        .use(_.run(30.seconds.toNanos) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    val timer = retrieveTimer(mr.snapshot.timers).values.head
    assert(timer.max == 30.seconds.toJava)
    assert(mr.snapshot.nonEmpty)
    assert(timer.calls == 1)
  }

  test("9.timer disable") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("timer")(
          _.timer("timer", _.enable(false).withReservoir(new SlidingWindowReservoir(10))).map(_.kleisli))
        .use(_.run(10) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.lastOrError.unsafeRunSync()
    assert(mr.snapshot.isEmpty)
    assert(retrieveTimer(mr.snapshot.timers).isEmpty)
  }

  test("10.empty") {
    val mr = service
      .eventStream(_.adhoc.report)
      .map(checkJson)
      .mapFilter(eventFilters.metricReport)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(mr.snapshot.isEmpty)
  }

  test("11.conflict name") {
    val mr = service
      .eventStream(agent =>
        agent.facilitate("same.name") { mtx =>
          val exec = for {
            c1 <- mtx.counter("counter")
            c2 <- mtx.counter("counter")
          } yield c1.inc(1) >> c2.inc(2)
          exec.use(r => r *> agent.adhoc.report)
        })
      .map(checkJson)
      .mapFilter(eventFilters.metricReport)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(mr.snapshot.hasDuplication)
    val counts: Map[MetricID, Long] = retrieveCounter(mr.snapshot.counters)
    assert(counts.values.toList.contains(1L))
    assert(counts.values.toList.contains(2L))
  }

  test("12.measured.retry - give up") {
    val sm = service.eventStream { agent =>
      agent
        .retry(_.isWorthRetry(tv => agent.herald.soleWarn(tv.value)(tv.tick).as(true)))
        .use(_.apply(IO.raiseError[Int](new Exception)) *> agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.serviceMessage).compile.toList.unsafeRunSync()
    assert(sm.size == 1)
  }

  test("13.measured.retry - unworthy retry") {
    val sm = service.eventStream { agent =>
      agent
        .retry(_.withPolicy(_.fixedDelay(1000.second).limited(2)).isWorthRetry(tv =>
          agent.herald.soleWarn(tv.value)(tv.tick).as(false)))
        .use(_.apply(IO.raiseError[Int](new Exception)) *> agent.adhoc.report)
    }.map(checkJson).mapFilter(eventFilters.serviceMessage).compile.toList.unsafeRunSync()

    assert(sm.size == 1)
  }
}
