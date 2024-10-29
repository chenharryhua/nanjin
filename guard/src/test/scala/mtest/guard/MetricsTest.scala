package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.MeasurementUnit.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.event.{Normalized, UnitNormalization}
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
        .withMetricReport(Policy.crontab(_.secondly))
        .disableHttpServer
        .disableJmx)

  test("1.lazy counting") {
    val last = task
      .service("delta")
      .updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))
      .eventStream(ag =>
        ag.facilitate("one")(_.action(IO(0)).buildWith(identity)).use(_.run(())) >> IO.sleep(10.minutes))
      .map(checkJson)
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()
    assert(last.forall(_.asInstanceOf[MetricReport].snapshot.counters.isEmpty))
  }

  test("3.reset") {
    val last = task
      .service("reset")
      .eventStream { ag =>
        (ag.facilitate("one")(_.action(IO(0)).buildWith(identity)) >>
          ag.facilitate("two")(_.action(IO(1)).buildWith(identity)))
          .surround(ag.adhoc.report >> ag.adhoc.reset >> IO.sleep(10.minutes))
      }
      .map(checkJson)
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()

    assert(last.get.asInstanceOf[MetricReport].snapshot.counters.forall(_.count == 0))
  }

  test("4.show timestamp") {
    val s =
      task.updateConfig(_.withMetricReport(Policy.crontab(Cron.unsafeParse("0-59 * * ? * *"))))

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

  test("5.distinct symbol") {
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

  test("6.normalization") {
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

  test("7.normalization - 2") {
    val um = UnitNormalization(NJTimeUnit.SECONDS, None, None)

    assert(um.normalize(10.minutes) == Normalized(600.0, NJTimeUnit.SECONDS))
    assert(um.normalize(10.minutes.toJava) == Normalized(600.0, NJTimeUnit.SECONDS))
    assert(um.normalize(NJInformationUnit.BYTES, 1000) == Normalized(1000.0, NJInformationUnit.BYTES))
    assert(um.normalize(NJDataRateUnit.MEGABITS_SECOND, 1) == Normalized(1.0, NJDataRateUnit.MEGABITS_SECOND))
  }

}
