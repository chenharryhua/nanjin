package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxMonadErrorRethrow
import com.codahale.metrics.jvm.{JvmAttributeGaugeSet, MemoryUsageGaugeSet}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.Importance
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import com.github.chenharryhua.nanjin.guard.translators.Translator
import eu.timepit.refined.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class MetricsTest extends AnyFunSuite {
  val sg: ServiceGuard[IO] =
    TaskGuard[IO]("metrics")
      .service("delta")
      .updateConfig(_.withMetricReport(1.second))
      .addMetricSet(new MemoryUsageGaugeSet)
      .addMetricSet(new JvmAttributeGaugeSet)

  test("delta") {
    val last = sg
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
  test("full") {
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

  test("ongoing action alignment") {
    sg.updateConfig(_.withMetricReport(1.second))
      .eventStream { ag =>
        val one = ag.action("one", _.notice).retry(IO(0) <* IO.sleep(10.minutes)).run
        val two = ag.action("two", _.notice).retry(IO(0) <* IO.sleep(10.minutes)).run
        IO.parSequenceN(2)(List(one, two))
      }
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .evalTap(console.simple[IO])
      .interruptAfter(5.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("reset") {
    val last = sg.eventStream { ag =>
      val metric = ag.metrics
      ag.action("one", _.notice).retry(IO(0)).run >> ag
        .action("two", _.notice)
        .retry(IO(1))
        .run >> metric.fullReport >> metric.reset >> IO.sleep(10.minutes)
    }.evalTap(console(Translator.simpleText[IO]))
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()

    assert(last.get.asInstanceOf[MetricReport].snapshot.counterMap.size === 0)
  }

  test("Importance json") {
    val i1: Importance = Importance.Critical
    val i2: Importance = Importance.High
    val i3: Importance = Importance.Medium
    val i4: Importance = Importance.Low

    assert(i1.asJson.noSpaces === """ "Critical" """.trim)
    assert(i2.asJson.noSpaces === """ "High" """.trim)
    assert(i3.asJson.noSpaces === """ "Medium" """.trim)
    assert(i4.asJson.noSpaces === """ "Low" """.trim)
  }

}
