package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.action.NJAlert
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.observers.{console, logging}
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import com.github.chenharryhua.nanjin.guard.translators.Translator
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

import scala.concurrent.duration.DurationInt
import scala.util.Random

class PassThroughTest extends AnyFunSuite {
  val guard: ServiceGuard[IO] = TaskGuard[IO]("test").service("pass-through")

  test("1.counter") {
    val Some(last) = guard
      .updateConfig(_.withMetricReport(cron_1second))
      .eventStream { ag =>
        val counter = ag.counter("one/two/three/counter")
        (counter.inc(1).replicateA(3) >> counter.dec(2)).delayBy(1.second) >> ag.metrics.report
      }
      .filter(_.isInstanceOf[MetricReport])
      .compile
      .last
      .unsafeRunSync()
    assert(
      last
        .asInstanceOf[MetricReport]
        .snapshot
        .counters
        .find(_.metricId.metricName.digest == "59d2456f")
        .size == 1)
  }

  test("2.alert") {
    val Some(last) = guard
      .updateConfig(_.withMetricReport(cron_1hour))
      .eventStream { ag =>
        val alert: NJAlert[IO] = ag.alert("oops").withCounting
        alert.warn(Some("message")) >> alert.info(Some("message")) >> alert.error(Some("message")) >>
          ag.metrics.report
      }
      .filter(_.isInstanceOf[MetricReport])
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()
    assert(
      last
        .asInstanceOf[MetricReport]
        .snapshot
        .counters
        .find(_.metricId.metricName.digest == "d42eee33")
        .size == 1)
  }

  test("3.meter") {
    guard
      .updateConfig(_.withMetricReport(cron_1second))
      .eventStream { agent =>
        val meter = agent.meter("nj.test.meter", StandardUnit.BYTES_SECOND)
        (meter.mark(1000) >> agent.metrics.reset
          .whenA(Random.nextInt(3) == 1)).delayBy(1.second).replicateA(5)
      }
      .evalTap(logging(Translator.simpleText[IO]))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("4.histogram") {
    guard
      .updateConfig(_.withMetricReport(cron_1second))
      .eventStream { agent =>
        val meter = agent.histogram("nj.test.histogram", StandardUnit.BYTES_SECOND)
        IO(Random.nextInt(100).toLong).flatMap(meter.update).delayBy(1.second).replicateA(5)
      }
      .evalTap(logging(Translator.simpleText[IO]))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("5.gauge") {
    guard
      .updateConfig(_.withMetricReport(cron_1second))
      .eventStream { agent =>
        val g1 = agent.gauge("elapse").timed
        val g2 = agent.gauge("exception").register(IO.raiseError[Int](new Exception))
        val g3 = agent.gauge("good").register(Random.nextInt(10))
        g1.both(g2).both(g3).surround(IO.sleep(3.seconds))
      }
      .evalTap(console.simple[IO])
      .map {
        case event: MetricReport => assert(event.snapshot.gauges.size == 3)
        case _                   => ()
      }
      .compile
      .drain
      .unsafeRunSync()
  }
}
