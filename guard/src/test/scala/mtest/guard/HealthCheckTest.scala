package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.observers.console
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.jdk.DurationConverters.JavaDurationOps

class HealthCheckTest extends AnyFunSuite {
  val guard: TaskGuard[IO] =
    TaskGuard[IO]("health-check").updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))
  test("1.never health-check - should terminate") {
    val res: List[MetricReport] = guard
      .service("health-check")
      .eventStream { agent =>
        agent
          .facilitate("never")(
            _.metrics.healthCheck("health-never", _.withTimeout(2.seconds)).register(IO.never[Boolean])
          )
          .surround(IO.sleep(5.seconds))
      }
      .map(checkJson)
      .evalTap(console.text[IO])
      .evalMapFilter(e => IO(metricReport(e)))
      .compile
      .toList
      .unsafeRunSync()
    assert(res.nonEmpty)
    assert(res.last.took.toScala >= 2.seconds)
  }

  test("2.never gauge - should terminate") {
    val res = guard
      .service("gauge")
      .eventStream(gd =>
        gd.facilitate("never")(
          _.metrics.gauge("gauge-never", _.withTimeout(2.seconds)).register(IO.never[Boolean])
        ).surround(IO.sleep(5.seconds)))
      .map(checkJson)
      .evalTap(console.text[IO])
      .evalMapFilter(e => IO(metricReport(e)))
      .compile
      .toList
      .unsafeRunSync()
    assert(res.nonEmpty)
  }

  test("3.cost health-check") {
    val res = guard
      .service("health-check-cost")
      .eventStream(gd =>
        gd.facilitate("cost")(
          _.metrics
            .healthCheck("health-never", _.withTimeout(2.seconds))
            .register(IO.never[Boolean], Policy.fixedDelay(1.seconds), gd.zoneId)
        ).surround(IO.sleep(3.seconds)))
      .map(checkJson)
      .evalTap(console.text[IO])
      .evalMapFilter(e => IO(metricReport(e)))
      .compile
      .toList
      .unsafeRunSync()
    assert(res.size > 3)
  }

  test("4.cost gauge") {
    val res = guard
      .service("gauge-cost")
      .eventStream(gd =>
        gd.facilitate("cost")(
          _.metrics
            .gauge("gauge-never-cost", _.withTimeout(2.seconds))
            .register(IO.never[Boolean], Policy.fixedDelay(1.seconds), gd.zoneId)
        ).surround(IO.sleep(3.seconds)))
      .map(checkJson)
      .evalTap(console.text[IO])
      .evalMapFilter(e => IO(metricReport(e)))
      .compile
      .toList
      .unsafeRunSync()
    assert(res.size > 3)
  }
}
