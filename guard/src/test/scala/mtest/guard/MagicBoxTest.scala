package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.{policies, tickStream}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{
  ActionDone,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.*

class MagicBoxTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("test")
      .service("magic-box")
      .updateConfig(_.withRestartPolicy(policies.fixedDelay(0.1.seconds)))

  test("1.atomicBox operations") {
    TaskGuard
      .dummyAgent[IO]
      .use { ag =>
        val box = ag.atomicBox(0)
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

  test("2.signalBox operations") {
    TaskGuard
      .dummyAgent[IO]
      .use { ag =>
        val box = ag.signalBox(0)
        for {
          v1 <- box.getAndSet(-1)
          _ <- box.set(1) // 1
          v2 <- box.getAndUpdate(_ + 1) // 2
          v3 <- box.updateAndGet(_ + 1) // 3
          _ <- box.update(_ + 1) // 4
          v6 <- box.get
        } yield {
          assert(v1 == 0)
          assert(v2 == 1)
          assert(v3 == 3)
          assert(v6 == 4)
        }
      }
      .unsafeRunSync()
  }

  test("3.signalBox should survive panic") {
    val List(a, b, c, d, e, f, g) =
      service.eventStream { agent =>
        val box = agent.signalBox(IO(10))
        for {
          _ <- box.updateAndGet(_ + 1)
          _ <- agent.action("publish", _.unipartite).retry(box.get).logOutput(_.asJson).run
          _ <- IO.raiseError[Int](new Exception)
        } yield ()
      }.debug().take(7).compile.toList.unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[ActionDone].notes.get.asNumber.flatMap(_.toInt).get == 11)
    assert(c.isInstanceOf[ServicePanic])
    assert(d.isInstanceOf[ServiceStart])
    assert(e.asInstanceOf[ActionDone].notes.get.asNumber.flatMap(_.toInt).get == 12)
    assert(f.isInstanceOf[ServicePanic])
    assert(g.isInstanceOf[ServiceStart])
  }

  test("4.atomicbox should survive panic") {
    val List(a, b, c, d, e, f, g) =
      service.eventStream { agent =>
        val box = agent.atomicBox(IO(10))
        for {
          _ <- box.getAndUpdate(_ + 1)
          _ <- agent.action("publish", _.unipartite).retry(box.get).logOutput(_.asJson).run
          _ <- IO.raiseError[Int](new Exception)
        } yield ()
      }.take(7).compile.toList.unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[ActionDone].notes.get.asNumber.flatMap(_.toInt).get == 11)
    assert(c.isInstanceOf[ServicePanic])
    assert(d.isInstanceOf[ServiceStart])
    assert(e.asInstanceOf[ActionDone].notes.get.asNumber.flatMap(_.toInt).get == 12)
    assert(f.isInstanceOf[ServicePanic])
    assert(g.isInstanceOf[ServiceStart])
  }

  test("5.signalBox should work as signal") {
    val List(a, c) =
      service.eventStream { agent =>
        val box = agent.signalBox(10)
        tickStream[IO](policies.fixedRate(0.1.seconds), ZoneId.systemDefault())
          .evalTap(_ => box.update(_ + 1))
          .interruptWhen(box.map(_ > 20))
          .compile
          .drain
      }.compile.toList.unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(c.isInstanceOf[ServiceStop])
  }

  test("6.atomicBox should eval initValue once") {
    var i: Int = 0
    val compute = if (i == 0) {
      i += 1
      IO(0)
    } else {
      i += 1
      IO.raiseError[Int](new Exception("never happen"))
    }

    val List(a) = service.eventStream { agent =>
      val box = agent.atomicBox(compute)
      tickStream[IO](policies.fixedDelay(0.5.seconds), ZoneId.systemDefault())
        .evalTap(_ => box.getAndUpdate(_ + 1))
        .compile
        .drain >> box.get.map(c => assert(c > 5))
    }.interruptAfter(3.second).compile.toList.unsafeRunSync()
    assert(i == 1)
    assert(a.isInstanceOf[ServiceStart])
  }

  test("7.signalBox should eval initValue once") {
    var i: Int = 0
    val compute = if (i == 0) {
      i += 1
      IO(0)
    } else {
      i += 1
      IO.raiseError[Int](new Exception("never happen"))
    }

    val List(a) = service.eventStream { agent =>
      val box = agent.signalBox(compute)
      tickStream[IO](policies.fixedDelay(0.5.seconds), ZoneId.systemDefault())
        .evalTap(_ => box.getAndUpdate(_ + 1))
        .compile
        .drain >> box.get.map(c => assert(c > 5))
    }.interruptAfter(3.second).compile.toList.unsafeRunSync()
    assert(i == 1)
    assert(a.isInstanceOf[ServiceStart])
  }

  test("8.atomicBox exception should trigger service panic") {

    val List(a, b, c) = service.eventStream { agent =>
      val box = agent.atomicBox(IO.raiseError[Int](new Exception))
      tickStream[IO](policies.fixedDelay(0.1.seconds), ZoneId.systemDefault())
        .evalTap(_ => box.getAndUpdate(_ + 1))
        .compile
        .drain
    }.take(3).compile.toList.unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServicePanic])
    assert(c.isInstanceOf[ServiceStart])
  }

  test("9.signalBox exception should trigger service panic") {

    val List(a, b, c) = service.eventStream { agent =>
      val box = agent.signalBox(IO.raiseError[Int](new Exception))
      tickStream[IO](policies.fixedDelay(0.1.seconds), ZoneId.systemDefault())
        .evalTap(_ => box.getAndUpdate(_ + 1))
        .compile
        .drain
    }.take(3).compile.toList.unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServicePanic])
    assert(c.isInstanceOf[ServiceStart])
  }

  test("10. signalBox release") {
    service.eventStream { agent =>
      val box = agent.signalBox(0)
      for {
        v0 <- box.get
        v1 <- box.updateAndGet(_ + 1)
        _ <- box.release
        v3 <- box.get
      } yield {
        assert(v0 == 0)
        assert(v1 == 1)
        assert(v3 == 0)
      }
    }.compile.drain.unsafeRunSync()
  }
  test("11. atomicBox release") {
    service.eventStream { agent =>
      val box = agent.atomicBox(100)
      for {
        v0 <- box.size
        v1 <- box.get
        v2 <- box.updateAndGet(_ + 1)
        _ <- box.release
        v3 <- box.size
        v4 <- box.get
      } yield {
        assert(v0 == 1)
        assert(v1 == 100)
        assert(v2 == 101)
        assert(v3 == 0)
        assert(v4 == 100)
      }
    }.compile.drain.unsafeRunSync()
  }
}
