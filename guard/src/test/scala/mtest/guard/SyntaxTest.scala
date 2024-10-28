package mtest.guard

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.Agent
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class SyntaxTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("task").service("service")

  test("1.builder syntax") {
    service.eventStream { agent =>
      val a0 = agent.facilitator("a0").action(unit_fun).buildWith(identity).use(_.run(()))
      val a1 = agent.facilitator("a1").action(fun1 _).buildWith(identity).use(_.run(1))
      val a2 = agent.facilitator("a2").action(fun2 _).buildWith(identity).use(_.run((1, 2)))
      val a3 = agent.facilitator("a3").action(fun3 _).buildWith(identity).use(_.run((1, 2, 3)))
      val a4 = agent.facilitator("a4").action(fun4 _).buildWith(identity).use(_.run((1, 2, 3, 4)))
      val a5 = agent.facilitator("a5").action(fun5 _).buildWith(identity).use(_.run((1, 2, 3, 4, 5)))

      a0 >> a1 >> a2 >> a3 >> a4 >> a5 >>
        agent.adhoc.report
    }.map(checkJson).compile.drain.unsafeRunSync()
  }

  test("2.should be in one namespace") {
    def job(ag: Agent[IO]): Resource[IO, Kleisli[IO, Int, Long]] = {
      val name: String = "app"
      val agent        = ag.facilitator("test").metrics
      for {
        timer <- agent.timer(name).map(_.kleisli((in: Int) => in.seconds))
        runner <- ag.facilitator(name).action(fun1 _).build
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
      .eventStream(ga => job(ga).use(_.run(1) >> ga.adhoc.report) >> ga.adhoc.report)
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
    def job(ag: Agent[IO]): Resource[IO, Kleisli[IO, Int, Long]] = {
      val name: String = "app2"
      val mtx          = ag.facilitator(name).metrics
      val msg          = ag.facilitator(name).messenger
      for {
        action <- ag.facilitator(name).action(fun1 _).buildWith(identity)
        histogram <- mtx.histogram(name, _.withUnit(_.BYTES))
        meter <- mtx.meter(name, _.withUnit(_.KILOBITS))
        counter <- mtx.counter(name, _.asRisk)
        timer <- mtx.timer(name)
        _ <- mtx.gauge(name).register(IO(0))
      } yield for {
        out <- action
        _ <- histogram.kleisli((_: Int) => out)
        _ <- meter.kleisli((_: Int) => out)
        _ <- counter.kleisli((_: Int) => out)
        _ <- timer.kleisli((in: Int) => in.milliseconds)
        _ <- Kleisli((in: Int) => msg.info(in))
      } yield out
    }

    val List(a: MetricReport, b: MetricReport) = service
      .eventStream(ga => job(ga).use(_.run(1) >> ga.adhoc.report) >> ga.adhoc.report)
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
}
