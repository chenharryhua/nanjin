package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxFlatMapOps
import com.codahale.metrics.SlidingTimeWindowArrayReservoir
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.event.{
  eventFilters,
  MeasurementUnit,
  NJDataRateUnit,
  NJDimensionlessUnit,
  NJEvent,
  NJInformationUnit,
  NJTimeUnit,
  Normalized,
  UnitNormalization
}
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import com.github.chenharryhua.nanjin.guard.translators.Translator
import cron4s.Cron
import eu.timepit.refined.auto.*
import io.circe.generic.JsonCodec
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite
import squants.time.Time

import java.time.{ZoneId, ZonedDateTime}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.Random

@JsonCodec
final case class SystemInfo(now: ZonedDateTime, on: Boolean, size: Int)

class MetricsTest extends AnyFunSuite {
  val zoneId: ZoneId = ZoneId.systemDefault()

  val service: ServiceGuard[IO] =
    TaskGuard[IO]("metrics")
      .updateConfig(_.withZoneId(zoneId).withHostName(HostName.local_host))
      .service("delta")
      .updateConfig(_.withMetricReport(policies.crontab(_.secondly)))

  test("1.lazy counting") {
    val last = service("delta")
      .updateConfig(_.withMetricReport(policies.crontab(_.secondly)))
      .eventStream(ag => ag.action("one", _.silent).retry(IO(0)).run >> IO.sleep(10.minutes))
      .evalTap(console.simple[IO])
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()
    assert(last.forall(_.asInstanceOf[MetricReport].snapshot.counters.isEmpty))
  }

  test("2.full") {
    val last = service
      .eventStream(ag => ag.action("one", _.counted).retry(IO(0)).run >> IO.sleep(10.minutes))
      .evalTap(console(Translator.simpleText[IO]))
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()
    assert(last.forall(_.asInstanceOf[MetricReport].snapshot.counters.nonEmpty))
  }

  test("3.ongoing action alignment") {
    service
      .updateConfig(_.withMetricReport(policies.crontab(_.secondly)))
      .eventStream { ag =>
        val one = ag.action("one", _.bipartite).retry(IO(0) <* IO.sleep(10.minutes)).run
        val two = ag.action("two", _.bipartite).retry(IO(0) <* IO.sleep(10.minutes)).run
        IO.parSequenceN(2)(List(one, two))
      }
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .evalTap(console.simple[IO].updateTranslator(_.filter(eventFilters.sampling(1))))
      .interruptAfter(5.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("4.reset") {
    val last = service.eventStream { ag =>
      val metric = ag.metrics
      ag.action("one", _.bipartite.timed.counted).retry(IO(0)).run >> ag
        .action("two", _.bipartite.timed.counted)
        .retry(IO(1))
        .run >> metric.report >> metric.reset >> IO.sleep(10.minutes)
    }.evalTap(console(Translator.simpleText[IO]))
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()

    assert(last.get.asInstanceOf[MetricReport].snapshot.counters.forall(_.count == 0))
  }

  test("5.show timestamp") {
    val s =
      service("timing").updateConfig(_.withMetricReport(policies.crontab(Cron.unsafeParse("0-59 * * ? * *"))))

    val s1 = s("s1").eventStream(_ => IO.never)
    val s2 = s("s2").eventStream(_ => IO.never)
    val s3 = s("s3").eventStream(_ => IO.never)
    val s4 = s("s4").eventStream(_ => IO.never)
    (IO.println(ZonedDateTime.now) >> IO.println("-----") >>
      s1.merge(s2)
        .merge(s3)
        .merge(s4)
        .filter(_.isInstanceOf[MetricReport])
        .map(_.asInstanceOf[MetricReport])
        .map(mr => (mr.index, mr.timestamp, mr.serviceParams.serviceName))
        .debug()
        .take(20)
        .compile
        .drain).unsafeRunSync()
  }

  test("6.namespace merge") {
    val name = "(name).space.test"
    TaskGuard[IO]("observers")
      .service("same_name_space")
      .withJmx(identity)
      .updateConfig(
        _.withRestartPolicy(policies.fixedDelay(1.hour)).withMetricReport(policies.crontab(_.secondly)))
      .eventStream { ag =>
        ag.gauge(name)
          .timed
          .surround(
            ag.action(name, _.bipartite.counted.timed).retry(IO(())).run >>
              ag.alert(name).counted.error("error") >>
              ag.alert(name).counted.warn("warn") >>
              ag.alert(name).counted.info("info") >>
              ag.meter(name, _.GIGABITS).counted.mark(100) >>
              ag.counter(name).inc(32) >>
              ag.counter(name).asRisk.inc(10) >>
              ag.histogram(name, _.BITS).counted.update(100) >>
              ag.metrics.report)
      }
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("7.distinct symbol") {
    import MeasurementUnit.*
    val symbols = List(
      DAYS,
      HOURS,
      MINUTES,
      SECONDS,
      MILLISECONDS,
      MICROSECONDS,
      NANOSECONDS,
      BYTES,
      KILOBYTES,
      MEGABYTES,
      GIGABYTES,
      TERABYTES,
      BITS,
      KILOBITS,
      MEGABITS,
      GIGABITS,
      TERABITS,
      BYTES_SECOND,
      KILOBYTES_SECOND,
      MEGABYTES_SECOND,
      GIGABYTES_SECOND,
      TERABYTES_SECOND,
      BITS_SECOND,
      KILOBITS_SECOND,
      MEGABITS_SECOND,
      GIGABITS_SECOND,
      TERABITS_SECOND,
      COUNT,
      PERCENT
    ).map(_.symbol)

    assert(symbols.distinct.size == symbols.size)
  }

  test("8.gauge") {
    service("gauge").eventStream { agent =>
      val gauge =
        agent.gauge("random").register(Random.nextInt(100)) >>
          agent.gauge("time").timed >>
          agent.gauge("ref").ref(IO.ref(0))

      gauge.use(box =>
        agent.ticks(policies.fixedDelay(1.seconds)).evalTap(_ => box.updateAndGet(_ + 1)).compile.drain)

    }.evalTap(console.simple[IO]).take(8).compile.drain.unsafeRunSync()
  }

  test("9.measurement unit") {
    implicitly[MeasurementUnit.DAYS.type =:= NJTimeUnit.DAYS.type]
    implicitly[MeasurementUnit.DAYS.Q =:= Time]
  }

  test("10.meter") {
    service("meter").eventStream { agent =>
      val meter = agent.meter("meter", _.MEGABYTES)
      meter.unsafeMark(10)
      meter.mark(20) >> agent.metrics.report
    }.evalTap(console.simple[IO]).compile.drain.unsafeRunSync()
  }

  test("11.histogram") {
    service("histo").eventStream { agent =>
      val histo = agent
        .histogram("histo", _.MEGABYTES)
        .counted
        .withReservoir(new SlidingTimeWindowArrayReservoir(2, TimeUnit.SECONDS))
      histo.unsafeUpdate(1)
      histo.update(2) >> histo.update(3) >> histo.update(4) >> agent.metrics.report
    }.evalTap(console.simple[IO]).compile.drain.unsafeRunSync()
  }

  test("12.normalization") {
    val um = UnitNormalization(
      NJTimeUnit.SECONDS,
      Some(NJInformationUnit.KILOBYTES),
      Some(NJDataRateUnit.KILOBITS_SECOND))

    assert(um.normalize(NJTimeUnit.MINUTES, 10) == Normalized(600.0, NJTimeUnit.SECONDS))
    assert(um.normalize(10.minutes) == Normalized(600.0, NJTimeUnit.SECONDS))
    assert(um.normalize(NJInformationUnit.BYTES, 1000) == Normalized(1.0, NJInformationUnit.KILOBYTES))
    assert(
      um.normalize(NJDataRateUnit.MEGABITS_SECOND, 1) == Normalized(1000.0, NJDataRateUnit.KILOBITS_SECOND))
    assert(um.normalize(NJDimensionlessUnit.COUNT, 1) == Normalized(1.0, NJDimensionlessUnit.COUNT))
  }

  test("13.normalization - 2") {
    val um = UnitNormalization(NJTimeUnit.SECONDS, None, None)

    assert(um.normalize(10.minutes) == Normalized(600.0, NJTimeUnit.SECONDS))
    assert(um.normalize(10.minutes.toJava) == Normalized(600.0, NJTimeUnit.SECONDS))
    assert(um.normalize(NJInformationUnit.BYTES, 1000) == Normalized(1000.0, NJInformationUnit.BYTES))
    assert(um.normalize(NJDataRateUnit.MEGABITS_SECOND, 1) == Normalized(1.0, NJDataRateUnit.MEGABITS_SECOND))
  }

}
