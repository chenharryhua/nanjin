package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxMonadErrorRethrow
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.{Importance, MetricSnapshotType}
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
    TaskGuard[IO]("metrics").service("delta").updateConfig(_.withMetricReport(1.second))
  test("delta") {
    val last = sg
      .updateConfig(_.withMetricSnapshotType(MetricSnapshotType.Delta))
      .eventStream(ag => ag.action("one")(_.silent).run(IO(0)) >> IO.sleep(10.minutes))
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
      .updateConfig(_.withMetricSnapshotType(MetricSnapshotType.Full))
      .eventStream(ag => ag.action("one")(_.withCounting).run(IO(0)) >> IO.sleep(10.minutes))
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
    sg.updateConfig(_.withMetricSnapshotType(MetricSnapshotType.Regular).withMetricReport(1.second))
      .eventStream { ag =>
        val one = ag.action("one")(_.notice).run(IO(0) <* IO.sleep(10.minutes))
        val two = ag.action("two")(_.notice).run(IO(0) <* IO.sleep(10.minutes))
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
    val last = sg
      .updateConfig(_.withMetricSnapshotType(MetricSnapshotType.Regular))
      .eventStream { ag =>
        val metric = ag.metrics
        ag.action("one")(_.notice).run(IO(0)) >> ag
          .action("two")(_.notice)
          .run(IO(1)) >> metric.fullReport >> metric.reset >> IO.sleep(10.minutes)
      }
      .evalTap(console(Translator.simpleText[IO]))
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

  test("MetricSnapshotType json") {
    val m1: MetricSnapshotType = MetricSnapshotType.Full
    val m2: MetricSnapshotType = MetricSnapshotType.Delta
    val m3: MetricSnapshotType = MetricSnapshotType.Regular

    assert(m1.asJson.noSpaces === """ "Full" """.trim)
    assert(m2.asJson.noSpaces === """ "Delta" """.trim)
    assert(m3.asJson.noSpaces === """ "Regular" """.trim)
  }

}
