package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.MeasurementUnit.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.event.{Normalized, UnitNormalization}
import com.github.chenharryhua.nanjin.guard.observers.console
import cron4s.Cron
import io.circe.generic.JsonCodec
import org.scalatest.funsuite.AnyFunSuite

import java.time.{ZoneId, ZonedDateTime}
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.ScalaDurationOps

@JsonCodec
final case class SystemInfo(now: ZonedDateTime, on: Boolean, size: Int)

class MetricsTest extends AnyFunSuite {
  val zoneId: ZoneId = ZoneId.systemDefault()

  val task: TaskGuard[IO] =
    TaskGuard[IO]("metrics").updateConfig(
      _.withZoneId(zoneId)
        .withHostName(HostName.local_host)
        .withMetricReport(policies.crontab(_.secondly))
        .disableHttpServer
        .disableJmx)

  test("1.lazy counting") {
    val last = task
      .service("delta")
      .updateConfig(_.withMetricReport(policies.crontab(_.secondly)))
      .eventStream(ag =>
        ag.action("one", _.silent).retry(IO(0)).buildWith(identity).use(_.run(())) >> IO.sleep(10.minutes))
      .map(checkJson)
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()
    assert(last.forall(_.asInstanceOf[MetricReport].snapshot.counters.isEmpty))
  }

  test("3.ongoing action alignment") {
    task
      .service("alignment")
      .updateConfig(_.withMetricReport(policies.crontab(_.secondly)))
      .eventStream { ag =>
        val one =
          ag.action("one", _.bipartite)
            .retry(IO(0) <* IO.sleep(10.minutes))
            .buildWith(identity)
            .use(_.run(()))
        val two =
          ag.action("two", _.bipartite)
            .retry(IO(0) <* IO.sleep(10.minutes))
            .buildWith(identity)
            .use(_.run(()))
        IO.parSequenceN(2)(List(one, two))
      }
      .map(checkJson)
      .interruptAfter(5.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("4.reset") {
    val last = task
      .service("reset")
      .eventStream { ag =>
        val metric = ag.metrics
        ag.action("one", _.bipartite.timed.counted).retry(IO(0)).buildWith(identity).use(_.run(())) >>
          ag.action("two", _.bipartite.timed.counted).retry(IO(1)).buildWith(identity).use(_.run(())) >>
          metric.report >> metric.reset >> IO.sleep(10.minutes)
      }
      .map(checkJson)
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()

    assert(last.get.asInstanceOf[MetricReport].snapshot.counters.forall(_.count == 0))
  }

  test("5.show timestamp") {
    val s =
      task.updateConfig(_.withMetricReport(policies.crontab(Cron.unsafeParse("0-59 * * ? * *"))))

    val s1 = s.service("s1").eventStream(_ => IO.never)
    val s2 = s.service("s2").eventStream(_ => IO.never)
    val s3 = s.service("s3").eventStream(_ => IO.never)
    val s4 = s.service("s4").eventStream(_ => IO.never)
    (IO.println(ZonedDateTime.now) >> IO.println("-----") >>
      s1.merge(s2)
        .merge(s3)
        .merge(s4)
        .mapFilter(metricReport)
        .map(mr => (mr.index, mr.timestamp, mr.serviceParams.serviceName))
        .debug()
        .take(20)
        .compile
        .drain).unsafeRunSync()
  }

  test("7.distinct symbol") {
    import com.github.chenharryhua.nanjin.guard.event.NJUnits.*
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

  test("11.normalization") {
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

  test("12.normalization - 2") {
    val um = UnitNormalization(NJTimeUnit.SECONDS, None, None)

    assert(um.normalize(10.minutes) == Normalized(600.0, NJTimeUnit.SECONDS))
    assert(um.normalize(10.minutes.toJava) == Normalized(600.0, NJTimeUnit.SECONDS))
    assert(um.normalize(NJInformationUnit.BYTES, 1000) == Normalized(1000.0, NJInformationUnit.BYTES))
    assert(um.normalize(NJDataRateUnit.MEGABITS_SECOND, 1) == Normalized(1.0, NJDataRateUnit.MEGABITS_SECOND))
  }

  test("14.jmx metric name") {
    task
      .service("metric name")
      .updateConfig(_.withJmx(identity))
      .eventStream { ga =>
        ga.action("a0{}[]()!@#$%^&+-_<>", _.bipartite.timed.counted)
          .retry(IO(0))
          .buildWith(identity)
          .use(_.run(())) >> ga.metrics.report
      }
      .map(checkJson)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("15.valid metric name") {
    val name = "a0{}[]()!@#$%^&+-_<>*?:;'"
    val Some(mr) = task
      .service("metric name")
      .eventStream { ga =>
        ga.action(name, _.bipartite.timed.counted)
          .retry(IO(0))
          .buildWith(identity)
          .use(_.run(()) >> ga.metrics.report)
      }
      .map(checkJson)
      .mapFilter(metricReport)
      .compile
      .last
      .unsafeRunSync()
    assert(mr.snapshot.timers.map(_.metricId.metricName.name).head == name)
  }

  test("16.dup") {
    task
      .service("dup")
      .eventStream { ga =>
        val jvm = ga.jvmGauge.classloader >> ga.jvmGauge.classloader
        jvm.surround(ga.metrics.report)
      }
      .map(checkJson)
      .mapFilter(metricReport)
      .evalTap(console.json[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("17.disable") {
    val mr = TaskGuard[IO]("nanjin")
      .service("disable")
      .eventStream { ag =>
        val go = for {
          _ <- ag.gauge("job", _.enable(false)).register(IO(1000000000))
          _ <- ag.healthCheck("job", _.enable(false)).register(IO(true))
          _ <- ag.timer("job", _.counted.enable(false)).evalMap(_.update(10.second).replicateA(100))
          _ <- ag
            .meter("job", _.withUnit(_.COUNT).counted.enable(false))
            .evalMap(_.update(10000).replicateA(100))
          _ <- ag.counter("job", _.asRisk.enable(false)).evalMap(_.inc(1000))
          _ <- ag
            .histogram("job", _.withUnit(_.BYTES).counted.enable(false))
            .evalMap(_.update(10000L).replicateA(100))
          _ <- ag.alert("job", _.counted.enable(false)).evalMap(_.error("alarm"))
          _ <- ag.flowMeter("job", _.withUnit(_.KILOBITS).counted.enable(false)).evalMap(_.update(200000))
          _ <- ag
            .action("job", _.timed.counted.bipartite.enable(false))
            .retry(IO(0))
            .buildWith(identity)
            .evalMap(_.run(()))
          _ <- ag
            .ratio("job", _.enable(false))
            .evalMap(f => f.incDenominator(50) >> f.incNumerator(79.999) >> f.incBoth(20.0, 50))
        } yield ()
        go.surround(ag.metrics.report)
      }
      .evalTap(console.text[IO])
      .mapFilter(metricReport)
      .compile
      .lastOrError
      .unsafeRunSync()
    val ss = mr.snapshot

    assert(ss.gauges.isEmpty)
    assert(ss.counters.isEmpty)
    assert(ss.timers.isEmpty)
    assert(ss.meters.isEmpty)
    assert(ss.histograms.isEmpty)
  }

}
