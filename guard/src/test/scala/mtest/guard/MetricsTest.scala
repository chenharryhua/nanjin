package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxFlatMapOps
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.observers.{console, sampling}
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import com.github.chenharryhua.nanjin.guard.translators.Translator
import cron4s.Cron
import eu.timepit.refined.auto.*
import io.circe.generic.JsonCodec
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

import java.time.{ZoneId, ZonedDateTime}
import scala.concurrent.duration.*
import scala.util.Random

@JsonCodec
final case class SystemInfo(now: ZonedDateTime, on: Boolean, size: Int)

class MetricsTest extends AnyFunSuite {
  val zoneId: ZoneId = ZoneId.systemDefault()

  val service: ServiceGuard[IO] =
    TaskGuard[IO]("metrics")
      .updateConfig(_.withZoneId(zoneId).withHostName(HostName.local_host))
      .service("delta")
      .withMetricReport(policies.crontab(cron_1second))

  test("1.lazy counting") {
    val last = service("delta")
      .withMetricReport(policies.crontab(cron_1second))
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
      .withMetricReport(policies.crontab(cron_1second))
      .eventStream { ag =>
        val one = ag.action("one", _.bipartite).retry(IO(0) <* IO.sleep(10.minutes)).run
        val two = ag.action("two", _.bipartite).retry(IO(0) <* IO.sleep(10.minutes)).run
        IO.parSequenceN(2)(List(one, two))
      }
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .evalTap(console.simple[IO].updateTranslator(_.filter(sampling(1))))
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
    val s = service("timing").withMetricReport(policies.crontab(Cron.unsafeParse("0-59 * * ? * *")))

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

  test("6.gauge") {
    service("gauge").eventStream { agent =>
      val gauge =
        agent.gauge("random").register(Random.nextInt(100)) >>
          agent.gauge("time").timed >>
          agent.gauge("ref").ref(IO.ref(0))

      gauge.use(box =>
        agent.ticks(policies.fixedDelay(1.seconds)).evalTap(_ => box.updateAndGet(_ + 1)).compile.drain)

    }.evalTap(console.simple[IO]).take(8).compile.drain.unsafeRunSync()
  }

  test("7. namespace merge") {
    val name = "(name).space.test"
    TaskGuard[IO]("observers")
      .service("same_name_space")
      .withRestartPolicy(constant_1hour)
      .withMetricReport(policies.crontab(cron_1second))
      .eventStream { ag =>
        ag.gauge(name)
          .timed
          .surround(
            ag.action(name, _.bipartite.counted.timed).retry(IO(())).run >>
              ag.alert(name).counted.error("error") >>
              ag.alert(name).counted.warn("warn") >>
              ag.alert(name).counted.info("info") >>
              ag.meter(name, StandardUnit.GIGABITS).counted.mark(100) >>
              ag.counter(name).inc(32) >>
              ag.counter(name).asRisk.inc(10) >>
              ag.histogram(name, StandardUnit.SECONDS).counted.update(64) >>
              ag.metrics.report)
      }
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }
}
