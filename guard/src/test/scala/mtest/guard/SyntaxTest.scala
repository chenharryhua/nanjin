package mtest.guard

import cats.Endo
import cats.data.Kleisli
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxSemigroup
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.event.retrieveMetricIds
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.Agent
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class SyntaxTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("task").service("service")

  test("1.builder syntax") {
    service.eventStream { agent =>
      val a0 = agent.action("a0").retry(unit_fun).buildWith(identity).use(_.run(()))
      val a1 = agent.action("a1").retry(fun1 _).buildWith(identity).use(_.run(1))
      val a2 = agent.action("a2").retry(fun2 _).buildWith(identity).use(_.run((1, 2)))
      val a3 = agent.action("a3").retry(fun3 _).buildWith(identity).use(_.run((1, 2, 3)))
      val a4 = agent.action("a4").retry(fun4 _).buildWith(identity).use(_.run((1, 2, 3, 4)))
      val a5 = agent.action("a5").retry(fun5 _).buildWith(identity).use(_.run((1, 2, 3, 4, 5)))
      val f0 = agent.action("f0").retryFuture(fun0fut).buildWith(identity).use(_.run(()))
      val f1 = agent.action("f1").retryFuture(fun1fut _).buildWith(identity).use(_.run(1))
      val f2 = agent.action("f2").retryFuture(fun2fut _).buildWith(identity).use(_.run((1, 2)))
      val f3 = agent.action("f3").retryFuture(fun3fut _).buildWith(identity).use(_.run((1, 2, 3)))
      val f4 = agent.action("f4").retryFuture(fun4fut _).buildWith(identity).use(_.run((1, 2, 3, 4)))
      val f5 = agent.action("f5").retryFuture(fun5fut _).buildWith(identity).use(_.run((1, 2, 3, 4, 5)))
      val d0 = agent.action("d0").delay(3).buildWith(identity).use(_.run(()))

      a0 >> a1 >> a2 >> a3 >> a4 >> a5 >>
        f0 >> f1 >> f2 >> f3 >> f4 >> f5 >>
        d0 >> agent.metrics.report
    }.map(checkJson).compile.drain.unsafeRunSync()
  }

  test("2.should be in one namespace") {
    def job(agent: Agent[IO]): Resource[IO, Kleisli[IO, Int, Long]] = {
      val name: String = "app"

      for {
        timer <- agent.timer(name).map(_.kleisli((in: Int) => in.seconds))
        runner <- agent.action(name, _.timed.counted).retry(fun1 _).buildWith(identity)
        histogram <- agent.histogram(name, _.withUnit(_.BYTES))
        meter <- agent.meter(name, _.withUnit(_.COUNT))
        counter <- agent.counter(name, _.asRisk)
        _ <- agent.gauge(name).register(IO(0))
      } yield for {
        _ <- timer
        out <- runner
        _ <- histogram.kleisli((_: Int) => out)
        _ <- meter.kleisli((_: Int) => out)
        _ <- counter.kleisli((_: Int) => out)
      } yield out
    }
    val List(a: MetricReport, b: MetricReport) = service
      .eventStream(ga => job(ga).use(_.run(1) >> ga.metrics.report) >> ga.metrics.report)
      .map(checkJson)
      .evalTap(console.text[IO])
      .evalMapFilter(e => IO(metricReport(e)))
      .compile
      .toList
      .unsafeRunSync()

    assert(a.snapshot.timers.nonEmpty)
    assert(a.snapshot.meters.nonEmpty)
    assert(a.snapshot.histograms.nonEmpty)
    assert(a.snapshot.gauges.nonEmpty)
    assert(a.snapshot.counters.nonEmpty)

    assert(b.snapshot.timers.isEmpty)
    assert(b.snapshot.meters.isEmpty)
    assert(b.snapshot.histograms.isEmpty)
    assert(b.snapshot.gauges.isEmpty)
    assert(b.snapshot.counters.isEmpty)

  }

  test("3.resource should be cleaned up") {
    def job(agent: Agent[IO]): Resource[IO, Kleisli[IO, Int, Long]] = {
      val name: String = "app2"
      for {
        action <- agent.action(name, _.timed.counted).retry(fun1 _).buildWith(identity)
        histogram <- agent.histogram(name, _.withUnit(_.BYTES))
        meter <- agent.meter(name, _.withUnit(_.KILOBITS))
        counter <- agent.counter(name, _.asRisk)
        timer <- agent.timer(name)
        alert <- agent.alert(name, _.counted)
        _ <- agent.gauge(name, _.withTag("abc")).register(IO(0))
      } yield for {
        out <- action
        _ <- histogram.kleisli((_: Int) => out)
        _ <- meter.kleisli((_: Int) => out)
        _ <- counter.kleisli((_: Int) => out)
        _ <- timer.kleisli((in: Int) => in.milliseconds)
        _ <- Kleisli((in: Int) => alert.info(in))
      } yield out
    }

    val List(a: MetricReport, b: MetricReport) = service
      .eventStream(ga => job(ga).use(_.run(1) >> ga.metrics.report) >> ga.metrics.report)
      .map(checkJson)
      .evalTap(console.text[IO])
      .evalMapFilter(e => IO(metricReport(e)))
      .compile
      .toList
      .unsafeRunSync()

    assert(a.snapshot.timers.nonEmpty)
    assert(a.snapshot.meters.nonEmpty)
    assert(a.snapshot.histograms.nonEmpty)
    assert(a.snapshot.gauges.nonEmpty)
    assert(a.snapshot.counters.nonEmpty)

    assert(b.snapshot.timers.isEmpty)
    assert(b.snapshot.counters.isEmpty)
    assert(b.snapshot.meters.isEmpty)
    assert(b.snapshot.histograms.isEmpty)
    assert(b.snapshot.gauges.isEmpty)
  }

  test("4.measurement") {
    val measurement  = "measurement"
    val name: String = "service-1"

    val mr: MetricReport = service.eventStream { ga =>
      val a = ga.alert(name, _.withMeasurement(measurement)).evalMap(_.info(1))
      val b = ga.counter(name, _.withMeasurement(measurement)).evalMap(_.inc(1))
      val c = ga.gauge(name, _.withMeasurement(measurement)).register(IO(1))
      val d = ga.healthCheck(name, _.withMeasurement(measurement)).register(IO(true))
      val e = ga.histogram(name, _.withMeasurement(measurement)).evalMap(_.update(1))
      val f = ga.meter(name, _.withMeasurement(measurement)).evalMap(_.update(1))
      val g = ga.timer(name, _.withMeasurement(measurement)).evalMap(_.update(1.second))
      (a |+| b |+| c |+| d |+| e |+| f |+| g).surround(ga.metrics.report)
    }.map(checkJson)
      .evalTap(console.text[IO])
      .evalMapFilter(e => IO(metricReport(e)))
      .compile
      .lastOrError
      .unsafeRunSync()

    val ms = retrieveMetricIds(mr.snapshot)
    assert(ms.map(_.metricName.measurement).forall(_ == measurement))
    assert(ms.map(_.metricName.name).forall(_ == name))
    assert(ms.size == 6)
  }

  test("5.config builders") {
    val alert: Endo[NJAlert.Builder]         = _.withMeasurement("alert").counted
    val counter: Endo[NJCounter.Builder]     = _.withMeasurement("counter").asRisk
    val gauge: Endo[NJGauge.Builder]         = _.withMeasurement("gauge").withTimeout(1.second)
    val hc: Endo[NJHealthCheck.Builder]      = _.withMeasurement("health-check").withTimeout(1.second)
    val histogram: Endo[NJHistogram.Builder] = _.withMeasurement("histogram").withUnit(_.BYTES)
    val meter: Endo[NJMeter.Builder]         = _.withMeasurement("meter").withUnit(_.KILOBITS)
    val timer: Endo[NJTimer.Builder]         = _.withMeasurement("timer")

    val name = "config"
    service.eventStream { ga =>
      val a = ga.alert(name, alert).evalMap(_.info(1))
      val b = ga.counter(name, counter).evalMap(_.inc(1))
      val c = ga.gauge(name, gauge).register(IO(1))
      val d = ga.healthCheck(name, hc).register(IO(true))
      val e = ga.histogram(name, histogram).evalMap(_.update(1))
      val f = ga.meter(name, meter).evalMap(_.update(1))
      val g = ga.timer(name, timer).evalMap(_.update(1.second))
      (a |+| b |+| c |+| d |+| e |+| f |+| g).use_ >> ga.metrics.report
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }
}
