package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.chrono.zones.singaporeTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ServiceMessage, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class ActionTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("action").updateConfig(_.withZoneId(singaporeTime)).service("action")

  test("kleisli") {
    val name = "kleisli"
    service.eventStream { agent =>
      val fac = agent.facilitator(name)
      val ga  = fac.metrics
      val calc = for {
        action <- fac.action((i: Int) => IO(i.toLong)).buildWith(identity)
        meter <- ga.meter(name)
        histogram <- ga.histogram(name)
        counter <- ga.counter(name)
      } yield for {
        _ <- meter.kleisli((in: Int) => in.toLong)
        out <- action
        _ <- histogram.kleisli((_: Int) => out)
        _ <- counter.kleisli((_: Int) => out)
      } yield out
      calc.use(_.run(10).replicateA(10) >> agent.adhoc.report)
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("null") {
    val List(a, b, c, d) = TaskGuard[IO]("logError")
      .service("no exception")
      .eventStream(
        _.facilitator("exception", _.withPolicy(Policy.fixedDelay(1.seconds).limited(2)))
          .action(IO.raiseError[Int](new Exception))
          .buildWith(_.tapError((_, _, _) => null.asInstanceOf[String].asJson))
          .use(_.run(())))
      .map(checkJson)
      .evalTap(console.text[IO])
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceStop])
  }
}
