package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{
  PassThrough,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import eu.timepit.refined.auto.*
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import scala.concurrent.duration.*

class MagicBoxTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("test").service("magic-box").withRestartPolicy(RetryPolicies.constantDelay[IO](0.1.seconds))

  test("1.signalBox should survive panic - option") {
    val List(a, b, c, d, e, f, g, h) = service.eventStream { agent =>
      val box    = agent.signalBox[Option[Int]](None)
      val broker = agent.broker("box")
      for {
        _ <- box.get.flatMap {
          case None               => box.set(Some(10))
          case Some(v) if v < 100 => box.update(_.map(_ * v))
          case Some(_)            => box.set(None)
        }
        _ <- box.get
          .flatMap(v => broker.passThrough(v.asJson))
          .flatMap(_ => IO.raiseError[Int](new Exception("oops")))
      } yield ()
    }.take(8).compile.toList.unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[PassThrough].value.as[Int].exists(_ == 10))
    assert(c.isInstanceOf[ServicePanic])
    assert(d.isInstanceOf[ServiceStart])
    assert(e.asInstanceOf[PassThrough].value.as[Int].exists(_ == 100))
    assert(f.isInstanceOf[ServicePanic])
    assert(g.isInstanceOf[ServiceStart])
    assert(h.asInstanceOf[PassThrough].value == Json.Null)
  }

  test("2.signalBox should survive panic - initial value") {
    val List(a, b, c, d, e, f, g, h) =
      service.eventStream { agent =>
        val box    = agent.signalBox(10)
        val broker = agent.broker("box")
        for {
          _ <- box.update(_ + 1)
          v <- box.get
          _ <- broker.passThrough(v.asJson).flatMap(_ => IO.raiseError[Int](new Exception("oops")))
        } yield ()
      }.take(8).compile.toList.unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[PassThrough].value.as[Int].exists(_ == 11))
    assert(c.isInstanceOf[ServicePanic])
    assert(d.isInstanceOf[ServiceStart])
    assert(e.asInstanceOf[PassThrough].value.as[Int].exists(_ == 12))
    assert(f.isInstanceOf[ServicePanic])
    assert(g.isInstanceOf[ServiceStart])
    assert(h.asInstanceOf[PassThrough].value.as[Int].exists(_ == 13))
  }

  test("3.signalBox work as signal") {
    val List(a, b, c) =
      service.eventStream { agent =>
        val box    = agent.signalBox(10)
        val broker = agent.broker("box")
        agent
          .ticks(RetryPolicies.constantDelay[IO](0.1.seconds))
          .evalTap(_ => box.update(_ + 1))
          .interruptWhen(box.map(_ > 20))
          .compile
          .drain >> box.get.flatMap(broker.passThrough(_))
      }.compile.toList.unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[PassThrough].value.as[Int].exists(_ == 21))
    assert(c.isInstanceOf[ServiceStop])
  }

  test("4.atomicbox should survive panic - effectful initial value") {
    val List(a, b, c, d, e, f, g, h) =
      service.eventStream { agent =>
        val box    = agent.atomicBox(IO(10))
        val broker = agent.broker("box")
        for {
          _ <- box.update(_ + 1)
          v <- box.get
          _ <- broker.passThrough(v.asJson).flatMap(_ => IO.raiseError[Int](new Exception("oops")))
        } yield ()
      }.take(8).compile.toList.unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[PassThrough].value.as[Int].exists(_ == 11))
    assert(c.isInstanceOf[ServicePanic])
    assert(d.isInstanceOf[ServiceStart])
    assert(e.asInstanceOf[PassThrough].value.as[Int].exists(_ == 12))
    assert(f.isInstanceOf[ServicePanic])
    assert(g.isInstanceOf[ServiceStart])
    assert(h.asInstanceOf[PassThrough].value.as[Int].exists(_ == 13))
  }

}
