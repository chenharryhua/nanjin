package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.funsuite.AnyFunSuite
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.retrieveFlowMeter

class FlowMeterTest extends AnyFunSuite {

  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("flow-meter").service("flow-meter")

  test("flow-meter") {
    val List(a, b) = service.eventStream { ga =>
      ga.flowMeter("flow", _.counted.withUnit(_.BYTES))
        .use(_.update(100).replicateA_(100) >> ga.metrics.report) >> ga.metrics.report
    }.map(checkJson).evalTap(console.text[IO]).mapFilter(metricReport).compile.toList.unsafeRunSync()

    val (ms, hs) = retrieveFlowMeter(a.snapshot.meters, a.snapshot.histograms)
    assert(ms.values.head.sum == 10000)
    assert(hs.values.head.updates == 100)

    val (ms2, hs2) = retrieveFlowMeter(b.snapshot.meters, b.snapshot.histograms)
    assert(ms2.isEmpty)
    assert(hs2.isEmpty)
  }
}
