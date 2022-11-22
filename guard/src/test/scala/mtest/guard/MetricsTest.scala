package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.codahale.metrics.jvm.ThreadStatesGaugeSet
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.Importance
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.observers.{console, sampling}
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import com.github.chenharryhua.nanjin.guard.translators.Translator
import cron4s.Cron
import eu.timepit.refined.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.Json
import io.circe.generic.JsonCodec
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import java.time.{ZoneId, ZonedDateTime}
import scala.concurrent.duration.*
import scala.util.Random

@JsonCodec
final case class SystemInfo(now: ZonedDateTime, on: Boolean, size: Int)

class MetricsTest extends AnyFunSuite {
  val zoneId: ZoneId = ZoneId.systemDefault()

  val service: ServiceGuard[IO] =
    TaskGuard[IO]("metrics")
      .updateConfig(_.withZoneId(zoneId))
      .service("delta")
      .updateConfig(_.withMetricReport(cron_1second))

  test("1.delta") {
    val last = service("delta")
      .updateConfig(_.withMetricReport(cron_1second))
      .addMetricSet(new ThreadStatesGaugeSet)
      .eventStream(ag => ag.action("one", _.silent).retry(IO(0)).run >> IO.sleep(10.minutes))
      .evalTap(console.simple[IO])
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()
    assert(last.forall(_.asInstanceOf[MetricReport].snapshot.counterMap.isEmpty))
  }

  test("2.full") {
    val last = service
      .eventStream(ag => ag.action("one", _.withCounting).retry(IO(0)).run >> IO.sleep(10.minutes))
      .evalTap(console(Translator.simpleText[IO]))
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()
    assert(last.forall(_.asInstanceOf[MetricReport].snapshot.counterMap.nonEmpty))
  }

  test("3.ongoing action alignment") {
    service
      .updateConfig(_.withMetricReport(cron_1second))
      .eventStream { ag =>
        val one = ag.action("one", _.notice).retry(IO(0) <* IO.sleep(10.minutes)).run
        val two = ag.action("two", _.notice).retry(IO(0) <* IO.sleep(10.minutes)).run
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
      ag.action("one", _.notice).retry(IO(0)).run >> ag
        .action("two", _.notice)
        .retry(IO(1))
        .run >> metric.report >> metric.reset >> IO.sleep(10.minutes)
    }.evalTap(console(Translator.simpleText[IO]))
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()

    assert(last.get.asInstanceOf[MetricReport].snapshot.counterMap.size === 0)
  }

  test("5.Importance json") {
    val i1: Importance = Importance.Critical
    val i2: Importance = Importance.Notice
    val i3: Importance = Importance.Silent
    val i4: Importance = Importance.Trivial

    assert(i1.asJson.noSpaces === """ "Critical" """.trim)
    assert(i2.asJson.noSpaces === """ "Notice" """.trim)
    assert(i3.asJson.noSpaces === """ "Silent" """.trim)
    assert(i4.asJson.noSpaces === """ "Trivial" """.trim)
  }

  test("6.show timestamp") {
    val s = service("timing").updateConfig(_.withMetricReport(Cron.unsafeParse("0-59 * * ? * *")))

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

  test("7.name conflict") {
    service("name.conflict")
      .updateConfig(_.withMetricReport(Cron.unsafeParse("0-59 * * ? * *")))
      .withRestartPolicy(RetryPolicies.constantDelay[IO](2.seconds))
      .eventStream { agent =>
        val name = "metric.name"
        agent.counter(name).inc(1) >>
          agent.meter(name).withCounting.mark(1) >>
          agent.histogram(name).withCounting.update(1) >>
          agent.broker(name).withCounting.passThrough(Json.fromString("broker.good")) >>
          agent.broker(name).asError.withCounting.passThrough(Json.fromString("broker.error")) >>
          agent.action(name, _.withTiming.withCounting).retry(IO(())).run.foreverM
      }
      .take(6)
      .debug()
      .compile
      .lastOrError
      .unsafeRunSync()
  }

  test("gauge") {
    service("gauge").eventStream { agent =>
      agent.gauge("random").register(Random.nextInt(100))
      val box = agent.blackBox(100000)
      agent.gauge("locker").register(box.get)

      for {
        state <- IO.ref(0)
        _ = agent.gauge("state").register(state)
        _ <- agent
          .ticks(RetryPolicies.constantDelay[IO](1.seconds))
          .evalTap(_ => state.update(_ + 1) >> box.update(_ + 1))
          .compile
          .drain
      } yield ()
    }.evalTap(console.simple[IO]).take(8).compile.drain.unsafeRunSync()
  }
}
