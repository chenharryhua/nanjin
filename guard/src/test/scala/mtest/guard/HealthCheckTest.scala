package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.observers.{console, logging}
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class HealthCheckTest extends AnyFunSuite {
  val guard: TaskGuard[IO] = TaskGuard[IO]("health-check")
  test("1.should receive 3 MetricsReport event") {
    val s :: a :: b :: c :: _ = guard
      .service("normal")
      .updateConfig(_.withMetricReport(policies.crontab(cron_2second)))
      .eventStream(gd => gd.action("cron", _.bipartite).retry(never_fun).run)
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .interruptAfter(9.second)
      .evalTap(logging.simple[IO])
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[MetricReport])
    assert(c.isInstanceOf[MetricReport])
    assert(c.isInstanceOf[MetricReport])
  }

  test("2.complete-test") {
    val s :: a :: b :: c :: d :: _ = guard
      .service("success-test")
      .updateConfig(_.withMetricReport(policies.crontab(cron_1second)))
      .eventStream(gd =>
        gd.action("a", _.bipartite).retry(IO(1)).run >>
          gd.action("b", _.bipartite).retry(never_fun).run)
      .evalTap(console.simple[IO])
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .evalTap(logging.simple[IO])
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionDone])
    assert(c.isInstanceOf[ActionStart])
    assert(d.isInstanceOf[MetricReport])
  }

  test("3.never complete") {
    val s :: a :: b :: c :: _ = guard
      .service("metrics-report")
      .updateConfig(_.withRestartPolicy(constant_1hour).withMetricReport(policies.crontab(cron_1second)))
      .eventStream(gd =>
        gd.action("not/fail/yet", _.bipartite.policy(constant_1hour)).retry(IO.raiseError(new Exception)).run)
      .interruptAfter(5.second)
      .evalTap(logging.simple[IO])
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[MetricReport])
  }

  test("4.metrics reset") {
    val list = guard
      .service("metrics-reset-test")
      .updateConfig(
        _.withMetricReport(policies.crontab(cron_2second)).withMetricReset(policies.crontab(cron_3second)))
      .eventStream(_.action("ok", _.silent).retry(never_fun).run)
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .evalTap(logging.simple[IO])
      .interruptAfter(7.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(list.nonEmpty)
  }
}
