package mtest.guard

import cats.data.Ior
import cats.effect.std.AtomicCell
import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.singaporeTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionFail, ActionStart, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

class ActionTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("action").updateConfig(_.withZoneId(singaporeTime)).service("action")

  test("kleisli") {
    val name = "kleisli"
    service.eventStream { ga =>
      val calc = for {
        action <- ga.action(name).retry((i: Int) => IO(i.toLong)).buildWith(identity)
        meter <- ga.meter(name)
        histogram <- ga.histogram(name)
        counter <- ga.counter(name)
        _ <- ga.gauge(name).timed
      } yield for {
        _ <- meter.kleisli((in: Int) => in.toLong)
        out <- action
        _ <- histogram.kleisli((_: Int) => out)
        _ <- counter.kleisli((_: Int) => out)
      } yield out
      calc.use(_.run(10).replicateA(10) >> ga.metrics.report)
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("null") {
    val List(a, b, c, d) = TaskGuard[IO]("logError")
      .service("no exception")
      .eventStream(
        _.action("exception", _.bipartite.counted)
          .retry(IO.raiseError[Int](new Exception))
          .buildWith(_.tapError((_, _) => null.asInstanceOf[String].asJson).tapInput(_ =>
            null.asInstanceOf[String].asJson))
          .use(_.run(())))
      .map(checkJson)
      .evalTap(console.text[IO])
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionFail])
    assert(d.isInstanceOf[ServiceStop])
  }

  test("ref vs atomic-cell vs timed") {
    service.eventStream { ga =>
      val r1 = for {
        ref <- Resource.eval(IO.ref(0))
        act <- ga.action("ref", _.timed).retry(ref.update(_ + 1)).buildWith(identity)
      } yield act

      val r2 = for {
        ref <- Resource.eval(AtomicCell[IO].of(0))
        act <- ga.action("atomic-cell", _.timed).retry(ref.update(_ + 1)).buildWith(identity)
      } yield act

      val r3 = for {
        act <- ga.action("timed", _.timed).retry(IO(1).timed).buildWith(identity)
      } yield act

      val num = 1_000_000
      r1.use(_.run(()).replicateA_(num) >> ga.metrics.report) &>
        r2.use(_.run(()).replicateA_(num) >> ga.metrics.report) &>
        r3.use(_.run(()).replicateA_(num) >> ga.metrics.report)
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("abc") {
    import io.circe.generic.auto.*
    val ior: Ior[Long, Long] = Ior.Both(1, 2)
    ior.asJson
  }

}
