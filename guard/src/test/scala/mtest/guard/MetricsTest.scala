package mtest.guard

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import org.scalatest.funsuite.AnyFunSuite
import cats.effect.unsafe.implicits.global
import com.codahale.metrics.MetricFilter
import com.github.chenharryhua.nanjin.guard.config.{DigestedName, MetricSnapshotType}
import com.github.chenharryhua.nanjin.guard.event.MetricsReport
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import com.github.chenharryhua.nanjin.guard.translators.Translator

import scala.concurrent.duration.*

class MetricsTest extends AnyFunSuite {
  val sg: ServiceGuard[IO] =
    TaskGuard[IO]("metrics").service("delta").updateConfig(_.withMetricReport(1.second).withBrief("test"))
  test("delta") {
    val last = sg
      .updateConfig(_.withMetricSnapshotType(MetricSnapshotType.Delta))
      .eventStream(ag => ag.span("one").run(IO(0)) >> IO.sleep(10.minutes))
      .evalTap(console[IO])
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()
    assert(last.forall(_.asInstanceOf[MetricsReport].snapshot.counterMap.isEmpty))
  }
  test("full") {
    val last = sg
      .updateConfig(_.withMetricSnapshotType(MetricSnapshotType.Full))
      .eventStream(ag => ag.span("one").run(IO(0)) >> IO.sleep(10.minutes))
      .evalTap(console(Translator.text[IO]))
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()
    assert(last.forall(_.asInstanceOf[MetricsReport].snapshot.counterMap.nonEmpty))
  }

  test("reset") {
    val last = sg
      .updateConfig(_.withMetricSnapshotType(MetricSnapshotType.Regular))
      .eventStream(ag =>
        ag.span("one").run(IO(0)) >> ag.span("two").run(IO(1)) >> ag.metrics.reset >> IO.sleep(10.minutes))
      .evalTap(console(Translator.text[IO]))
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()

    assert(last.get.asInstanceOf[MetricsReport].snapshot.counterMap.size === 0)
  }
}
