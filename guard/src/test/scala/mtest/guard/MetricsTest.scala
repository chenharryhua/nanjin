package mtest.guard

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import cats.kernel.Eq
import cats.syntax.all.*
import com.codahale.metrics.SlidingWindowReservoir
import com.github.chenharryhua.nanjin.common.resilience.Retry
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.metrics.MetricElement.CounterData
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.metrics.api.Meter
import com.github.chenharryhua.nanjin.guard.metrics.{MetricID, MetricName, retrieve}
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.jawn.decode
import org.scalatest.funsuite.AnyFunSuite
import squants.information.{Bytes, Information}
import squants.market.{AUD, Money}
import squants.time.{Milliseconds, Time}
import squants.{Dimensionless, Percent}

import java.time.{ZoneId, ZonedDateTime}
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps
final case class SystemInfo(now: ZonedDateTime, on: Boolean, size: Int)

class MetricsTest extends AnyFunSuite {
  val zoneId: ZoneId = ZoneId.systemDefault()

  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("metrics")
      .updateConfig(_.withZoneId(zoneId))
      .service("metrics")

  test("1.counter") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("counter")(_.counter("counter"))
        .use(_.inc(10) >> agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    assert(mr.snapshot.nonEmpty)
    assert(retrieve.counter(mr.snapshot.counters).values.head.value == 10)
    assert(retrieve.riskCounter(mr.snapshot.counters).values.isEmpty)
    assert(mr.index.isInstanceOf[Index.Adhoc])
  }

  test("2.counter risk") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("counter")(_.counter("counter", _.asRisk))
        .use(_.inc(10) >> agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    assert(retrieve.riskCounter(mr.snapshot.counters).values.head.value == 10)
    assert(retrieve.counter(mr.snapshot.counters).values.isEmpty)
  }

  test("3.counter disable") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("counter")(_.counter("counter", _.enable(false)))
        .use(_.inc(10) >> agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    assert(mr.snapshot.isEmpty)
    assert(retrieve.counter(mr.snapshot.counters).values.isEmpty)
    assert(retrieve.riskCounter(mr.snapshot.counters).values.isEmpty)
  }

  test("3b.unsafe counter") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("counter")(_.unsafeCounter("counter"))
        .use { counter =>
          IO.delay(counter.unsafeInc(10)) >> agent.adhoc.report.void
        }
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()

    assert(retrieve.counter(mr.snapshot.counters).values.head.value == 10)
    assert(retrieve.riskCounter(mr.snapshot.counters).values.isEmpty)
  }

  test("3c.counter reset by policy") {
    val snapshots = service.eventStream { agent =>
      agent
        .facilitate("counter")(_.counter("counter", _.withPolicy(_.fixedDelay(200.millis))))
        .use { counter =>
          counter.inc(10) >>
            agent.adhoc.report >>
            IO.sleep(500.millis) >>
            counter.inc(3) >>
            agent.adhoc.report
        }
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.toList.unsafeRunSync()

    assert(snapshots.size == 2)

    val first = retrieve.counter(snapshots.head.snapshot.counters).values.head.value
    val second = retrieve.counter(snapshots(1).snapshot.counters).values.head.value

    assert(first == 10)
    assert(second == 3)
  }

  test("4.meter") {
    val mr = service.eventStream { agent =>
      val meter: Resource[IO, Meter[IO]] = agent.facilitate("meter")(_.meter("meter", _.withUnit(AUD)))
      meter.use(m => m.mark(10) >> m.mark(20) >> agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val meter = retrieve.meter(mr.snapshot.meters).values.head
    assert(mr.snapshot.nonEmpty)
    assert(meter.aggregate == 30)
    assert(meter.squants.unitSymbol == AUD.symbol)
    assert(meter.squants.dimensionName == Money.name)
  }

  test("4b.unsafe meter") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("meter")(_.unsafeMeter("meter"))
        .use { meter =>
          IO.delay(meter.unsafeMark(10)) >> agent.adhoc.report.void
        }
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()

    val meter = retrieve.meter(mr.snapshot.meters).values.head
    assert(mr.snapshot.nonEmpty)
    assert(meter.aggregate == 10)
  }

  test("5.meter disable") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("meter")(_.meter("meter", _.enable(false)))
        .use(_.mark(10) >> agent.adhoc.report.void)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    assert(mr.snapshot.isEmpty)
    assert(retrieve.meter(mr.snapshot.meters).isEmpty)
  }

  test("6.histogram") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("histogram")(_.histogram("histogram", _.withUnit(Bytes)))
        .use(m => m.update(10) >> m.update(20) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val histo = retrieve.histogram(mr.snapshot.histograms).values.head
    assert(mr.snapshot.nonEmpty)
    assert(histo.updates == 2)
    assert(histo.max == 20)
    assert(histo.squants.unitSymbol == Bytes.symbol)
    assert(histo.squants.dimensionName == Information.name)
  }

  test("6b.unsafe histogram") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("histogram")(_.unsafeHistogram("histogram"))
        .use { histogram =>
          IO.delay(histogram.unsafeUpdate(10)) >> agent.adhoc.report.void
        }
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()

    val histo = retrieve.histogram(mr.snapshot.histograms).values.head
    assert(mr.snapshot.nonEmpty)
    assert(histo.updates == 1)
    assert(histo.max == 10)
  }

  test("7.histogram timer") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("histogram")(_.histogram("histogram", _.withUnit(Milliseconds)))
        .use(m => m.update(1030) >> m.update(200) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val histo = retrieve.histogram(mr.snapshot.histograms).values.head
    assert(mr.snapshot.nonEmpty)
    assert(histo.updates == 2)
    assert(histo.max == 1030)
    assert(histo.squants.unitSymbol == Milliseconds.symbol)
    assert(histo.squants.dimensionName == Time.name)
  }

  test("8.histogram percent") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("histogram")(_.histogram("histogram", _.withUnit(Percent)))
        .use(m => m.update(30) >> m.update(50) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val histo = retrieve.histogram(mr.snapshot.histograms).values.head
    assert(mr.snapshot.nonEmpty)
    assert(histo.updates == 2)
    assert(histo.max == 50)
    assert(histo.squants.unitSymbol == Percent.symbol)
    assert(histo.squants.dimensionName == Dimensionless.name)
  }

  test("9.histogram disable") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("histogram")(_.histogram("histogram", _.enable(false).withUnit(Bytes)))
        .use(_.update(10) >>
          agent.adhoc.report)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    assert(mr.snapshot.isEmpty)
    assert(retrieve.histogram(mr.snapshot.histograms).isEmpty)
  }

  test("10.timer") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("timer")(_.timer("timer"))
        .use(_.elapsedNano(30.seconds.toNanos) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    val timer = retrieve.timer(mr.snapshot.timers).values.head
    assert(timer.max == 30.seconds.toJava)
    assert(mr.snapshot.nonEmpty)
    assert(timer.calls == 1)
  }

  test("10b.unsafe timer") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("timer")(_.unsafeTimer("timer"))
        .use { timer =>
          IO.delay(timer.unsafeElapsedNano(10)) >> agent.adhoc.report.void
        }
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()

    val timer = retrieve.timer(mr.snapshot.timers).values.head
    assert(mr.snapshot.nonEmpty)
    assert(timer.calls == 1)
    assert(timer.max == 10.nanos.toJava)
  }

  test("11.timer disable") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("timer")(_.timer("timer", _.enable(false).withReservoir(new SlidingWindowReservoir(10))))
        .use(_.elapsedNano(10) >> agent.adhoc.report)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()
    assert(mr.snapshot.isEmpty)
    assert(retrieve.timer(mr.snapshot.timers).isEmpty)
  }

  test("12.empty") {
    val mr = service
      .eventStream(_.adhoc.report)
      .map(checkJson)
      .mapFilter(Event.metricsSnapshot.getOption)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(mr.snapshot.isEmpty)
  }

  test("13.conflict name") {
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
      .mapFilter(Event.metricsSnapshot.getOption)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(mr.snapshot.hasDuplication)
    val counts: Map[MetricID, CounterData] = retrieve.counter(mr.snapshot.counters)
    assert(counts.values.toList.map(_.value).contains(1L))
    assert(counts.values.toList.map(_.value).contains(2L))
  }

  test("14.measured.retry - give up") {
    val sm = service.eventStream { agent =>
      agent
        .retry(_.withDecision(tv => IO(tv.followPolicy)))
        .use(_.apply(IO.raiseError[Int](new Exception)) *> agent.adhoc.report)
    }.map(checkJson).mapFilter(Event.reportedEvent.getOption).compile.toList.unsafeRunSync()
    assert(sm.isEmpty)
  }

  test("15.measured.retry - unworthy retry") {
    val sm = service.eventStream { agent =>
      agent
        .retry(_.withPolicy(_.fixedDelay(1000.second).limited(2)).withDecision(ra =>
          IO(ra.giveUp).flatTap(d => agent.herald.warn(d, ra.cause))))
        .use(_.apply(IO.raiseError[Int](new Exception)) *> agent.adhoc.report)
    }.map(checkJson).mapFilter(Event.reportedEvent.getOption).compile.toList.unsafeRunSync()

    assert(sm.size == 1)
  }

  test("16.MetricName") {
    val m1 = decode[MetricName]("""{"name" : "foo","age" : 5,"uniqueToken" : 1}""").toOption.get
    val m2 = decode[MetricName]("""{"name" : "foo","age" : 5,"uniqueToken" : 1}""").toOption.get
    val m3 = decode[MetricName]("""{"name" : "foo","age" : 5,"uniqueToken" : 2}""").toOption.get

    val ek = Eq[MetricName]
    assert(ek.eqv(m1, m2))
    assert(!ek.eqv(m1, m3))
    assert(m1 == m2)
    assert(m1 != m3)
    assert(m1 =!= m3)
  }

  test("17.meter + counter") {
    val List(report) = service.eventStream { agent =>
      val run = agent.facilitate("abc-xyz-123") { mtx =>
        for {
          m <- mtx.meter("aaa-bbb")
          c <- mtx.counter("aaa-bbb")
        } yield m.mark(10) >> c.inc(10)
      }
      run.use(a => a >> agent.adhoc.report)
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.toList.unsafeRunSync()
    assert(report.index.isInstanceOf[Index.Adhoc])
  }
}
