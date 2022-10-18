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
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class MetricsTest extends AnyFunSuite {
  val sg: ServiceGuard[IO] =
    TaskGuard[IO]("metrics").service("delta").updateConfig(_.withMetricReport(1.second))

  test("1.delta") {
    val last = TaskGuard[IO]("metrics")
      .service("delta")
      .updateConfig(_.withMetricReport(1.second))
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
    val last = sg
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
    sg.updateConfig(_.withMetricReport(1.second))
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
    val last = sg.eventStream { ag =>
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
    val i2: Importance = Importance.High
    val i3: Importance = Importance.Medium
    val i4: Importance = Importance.Low

    assert(i1.asJson.noSpaces === """ "Critical" """.trim)
    assert(i2.asJson.noSpaces === """ "High" """.trim)
    assert(i3.asJson.noSpaces === """ "Medium" """.trim)
    assert(i4.asJson.noSpaces === """ "Low" """.trim)
  }

  ignore("timing") {
    val s = TaskGuard[IO]("metrics")
      .service("timing")
      .updateConfig(_.withMetricReport(Cron.unsafeParse("0-59 * * ? * *")))

    val s1 = s("s1").eventStream(_ => IO.never)
    val s2 = s("s2").eventStream(_ => IO.never)
    val s3 = s("s3").eventStream(_ => IO.never)
    val s4 = s("s4").eventStream(_ => IO.never)

    s1.merge(s2).merge(s3).merge(s4).evalTap(console.simple[IO]).compile.drain

  }

}
