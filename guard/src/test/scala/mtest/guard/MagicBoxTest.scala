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
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import scala.concurrent.duration.*

class MagicBoxTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("test").service("magic-box").withRestartPolicy(RetryPolicies.constantDelay[IO](0.1.seconds))

  test("1.atomicBox operations") {
    TaskGuard
      .dummyAgent[IO]
      .use { ag =>
        val box = ag.atomicBox(IO(0))
        for {
          v1 <- box.getAndSet(-1)
          _ <- box.set(1) // 1
          v2 <- box.getAndUpdate(_ + 1) // 2
          v3 <- box.updateAndGet(_ + 1) // 3
          _ <- box.update(_ + 1) // 4
          _ <- box.evalUpdate(x => IO(x + 1)) // 5
          v4 <- box.evalGetAndUpdate(x => IO(x + 1)) // 6
          v5 <- box.evalUpdateAndGet(x => IO(x + 1)) // 7
          v6 <- box.get
        } yield {
          assert(v1 == 0)
          assert(v2 == 1)
          assert(v3 == 3)
          assert(v4 == 5)
          assert(v5 == 7)
          assert(v6 == 7)
        }
      }
      .unsafeRunSync()
  }

  test("2.refBox should survive panic") {
    val List(a, b, c, d, e, f, g, h) =
      service.eventStream { agent =>
        val box    = agent.refBox(10)
        val broker = agent.broker("box")
        for {
          v <- box.updateAndGet(_ + 1)
          _ <- broker.passThrough(v.asJson).flatMap(_ => IO.raiseError[Int](new Exception))
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

  test("3.signalBox should survive panic") {
    val List(a, b, c, d, e, f, g, h) =
      service.eventStream { agent =>
        val box    = agent.signalBox(10)
        val broker = agent.broker("box")
        for {
          v <- box.updateAndGet(_ + 1)
          _ <- broker.passThrough(v.asJson).flatMap(_ => IO.raiseError[Int](new Exception))
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

  test("4.atomicbox should survive panic") {
    val List(a, b, c, d, e, f, g, h) =
      service.eventStream { agent =>
        val box    = agent.atomicBox(IO(10))
        val broker = agent.broker("box")
        for {
          _ <- IO.println("waiting for cats.effect 3.4.2")
          v <- box.getAndUpdate(_ + 1)
          _ <- broker.passThrough(v.asJson).flatMap(_ => IO.raiseError[Int](new Exception))
        } yield ()
      }.take(8).compile.toList.unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[PassThrough].value.as[Int].exists(_ == 10))
    assert(c.isInstanceOf[ServicePanic])
    assert(d.isInstanceOf[ServiceStart])
    assert(e.asInstanceOf[PassThrough].value.as[Int].exists(_ == 11))
    assert(f.isInstanceOf[ServicePanic])
    assert(g.isInstanceOf[ServiceStart])
    assert(h.asInstanceOf[PassThrough].value.as[Int].exists(_ == 12))
  }

  test("5.signalBox should work as signal") {
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

  test("6. atomicBox should eval initValue once") {
    var i: Int = 0
    val compute = if (i == 0) {
      i += 1
      IO(0)
    } else {
      i += 1
      IO.raiseError[Int](new Exception("never happen"))
    }

    service.eventStream { agent =>
      val box = agent.atomicBox(compute)
      agent.gauge("box").register(box.get)
      agent
        .ticks(RetryPolicies.constantDelay[IO](0.1.seconds))
        .evalTap(_ => box.getAndUpdate(_ + 1).flatMap(IO.println))
        .compile
        .drain
    }.interruptAfter(3.second).compile.toList.unsafeRunSync()
    assert(i == 1)
  }

  test("7. atomicBox exception should trigger service panic") {

    val List(a, b, c) = service.eventStream { agent =>
      val box = agent.atomicBox(IO.raiseError[Int](new Exception))
      agent
        .ticks(RetryPolicies.constantDelay[IO](0.1.seconds))
        .evalTap(_ => box.getAndUpdate(_ + 1))
        .compile
        .drain
    }.take(3).compile.toList.unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServicePanic])
    assert(c.isInstanceOf[ServiceStart])
  }
}
