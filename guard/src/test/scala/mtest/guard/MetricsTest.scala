package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.MetricSnapshotType
import com.github.chenharryhua.nanjin.guard.event.MetricReport
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import com.github.chenharryhua.nanjin.guard.translators.Translator
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class MetricsTest extends AnyFunSuite {
  val sg: ServiceGuard[IO] =
    TaskGuard[IO]("metrics").service("delta").updateConfig(_.withMetricReport(1.second))
  test("delta") {
    val last = sg
      .updateConfig(_.withMetricSnapshotType(MetricSnapshotType.Delta))
      .eventStream(ag => ag.span("one").run(IO(0)) >> IO.sleep(10.minutes))
      .evalTap(console.verbose[IO])
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()
    assert(last.forall(_.asInstanceOf[MetricReport].snapshot.counterMap.isEmpty))
  }
  test("full") {
    val last = sg
      .updateConfig(_.withMetricSnapshotType(MetricSnapshotType.Full))
      .eventStream(ag => ag.span("one").updateConfig(_.withCounting).run(IO(0)) >> IO.sleep(10.minutes))
      .evalTap(console(Translator.text[IO]))
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()
    assert(last.forall(_.asInstanceOf[MetricReport].snapshot.counterMap.nonEmpty))
  }

  test("reset") {
    val last = sg
      .updateConfig(_.withMetricSnapshotType(MetricSnapshotType.Regular))
      .eventStream { ag =>
        val metric = ag.metrics
        ag.span("one").run(IO(0)) >> ag.span("two").run(IO(1)) >> metric.fullReport >> metric.reset >> IO.sleep(
          10.minutes)
      }
      .evalTap(console(Translator.text[IO]))
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()

    assert(last.get.asInstanceOf[MetricReport].snapshot.counterMap.size === 0)
  }
}
