package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import eu.timepit.refined.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

final case class MyException() extends Exception("my exception")

class RetryTest extends AnyFunSuite {

  val serviceGuard: ServiceGuard[IO] =
    TaskGuard[IO]("retry-guard").service("retry test").updateConfig(_.withConstantDelay(1.seconds))

  test("1.retry - success trivial") {
    val Vector(s, c) = serviceGuard.eventStream { gd =>
      gd.action(_.withFullJitterBackoff(1.second, 3))
        .retry((x: Int, y: Int, z: Int) => IO(x + y + z))
        .logOutput((a, _) => a.asJson)
        .withWorthRetry(_ => true)
        .run(1, 1, 1)("succ-trivial")

    }.evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow).compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(c.isInstanceOf[ServiceStop])
  }

  test("2.retry - success notice") {
    val Vector(s, a, b, c, d, e, f, g) = serviceGuard.eventStream { gd =>
      val ag = gd
        .action(_.notice.withExponentialBackoff(1.second, 3))
        .retry((v: Int, w: Int, x: Int, y: Int, z: Int) => IO(v + w + x + y + z))
        .logInput
        .withWorthRetry(_ => true)
      List(1, 2, 3).traverse(i => ag.run(i, i, i, i, i)("all-succ"))
    }.evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow).compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionSucc])
    assert(c.isInstanceOf[ActionStart])
    assert(d.isInstanceOf[ActionSucc])
    assert(e.isInstanceOf[ActionStart])
    assert(f.isInstanceOf[ActionSucc])
    assert(g.isInstanceOf[ServiceStop])
  }

  test("3.retry - all fail") {
    val Vector(s, a, b, c, d, e, f, g, h, i, j) = serviceGuard.eventStream { gd =>
      val ag = gd
        .action(_.notice.withConstantDelay(0.1.second, 1))
        .retry((_: Int, _: Int, _: Int) => IO.raiseError[Int](new Exception))
        .logOutput((in, out) => (in._3, out).asJson)
        .logOutput((in, out) => (in, out).asJson)

      List(1, 2, 3).traverse(i => ag.run(i, i, i)("all-fail").attempt)
    }.evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow).compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionFail])
    assert(d.isInstanceOf[ActionStart])
    assert(e.isInstanceOf[ActionRetry])
    assert(f.isInstanceOf[ActionFail])
    assert(g.isInstanceOf[ActionStart])
    assert(h.isInstanceOf[ActionRetry])
    assert(i.isInstanceOf[ActionFail])
    assert(j.isInstanceOf[ServiceStop])
  }

  test("4.retry - should retry 2 times when operation fail") {
    var i = 0
    val Vector(s, a, b, c, d, e) = serviceGuard.eventStream { gd =>
      gd.action(_.notice.withFullJitterBackoff(1.second, 3))
        .retry((_: Int) =>
          IO(if (i < 2) {
            i += 1; throw new Exception
          } else i))
        .logOutput((a, _) => a.asJson)
        .run(1)("1-time-succ")
    }.evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow).compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionSucc])
    assert(e.isInstanceOf[ServiceStop])
  }

  test("5.retry - should retry 2 times when operation fail - low") {
    var i = 0
    val Vector(s, b, c, d, e, f) = serviceGuard.eventStream { gd =>
      gd.action(_.critical.withFullJitterBackoff(1.second, 30))
        .retry((_: Int) =>
          IO(if (i < 2) {
            i += 1
            throw new Exception
          } else i))
        .logInput(_.asJson)
        .run(1)("1-time-succ")
    }.compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionSucc])
    assert(f.isInstanceOf[ServiceStop])
  }

  test("6.retry - should escalate to up level if retry failed") {
    val Vector(s, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd.action(_.withFibonacciBackoff(0.1.second, 3))
          .retry((_: Int) => IO.raiseError[Int](new Exception("oops")))
          .logInput
          .run(1)("escalate-after-3-times")
      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionFail])
    assert(f.isInstanceOf[ServicePanic])
  }

  test("7.retry - Null pointer exception") {
    val s :: b :: c :: d :: e :: _ = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream(ag =>
        ag.action(_.withCapDelay(1.second).withConstantDelay(100.second, 2))
          .retry(IO.raiseError[Int](new NullPointerException))
          .logOutput
          .run("null exception"))
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionFail])
    assert(e.isInstanceOf[ServicePanic])
  }

  test("8.retry - predicate - should retry") {
    val Vector(s, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd.action(_.withFibonacciBackoff(0.1.second, 3))
          .retry(IO.raiseError(MyException()))
          .withWorthRetry(_.isInstanceOf[MyException])
          .run("predicate")
      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionFail])
    assert(f.isInstanceOf[ServicePanic])
  }

  test("9.retry - isWorthRetry - should not retry") {
    val Vector(s, a, b, c) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd.action(_.notice.withFibonacciBackoff(0.1.second, 3))
          .retry(IO.raiseError(new Exception))
          .withWorthRetry(_.isInstanceOf[MyException])
          .run("predicate")
      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionFail])
    assert(c.isInstanceOf[ServicePanic])
  }

  test("10.quasi syntax") {
    serviceGuard.eventStream { ag =>
      ag.quasi(3)(IO("a"), IO("b")).value >>
        ag.quasi(3, List(IO("a"), IO("b"))).value >>
        ag.quasi(List(IO("a"), IO("b"))).value >>
        ag.quasi(IO("a"), IO("b")).value >>
        ag.quasi(IO.print("a"), IO.print("b")).value >>
        ag.quasi(3)(IO.print("a"), IO.print("b")).value
    }
  }

  test("12.retry - nonterminating - should retry") {
    val a :: b :: c :: d :: e :: f :: _ = serviceGuard
      .updateConfig(_.withConstantDelay(1.second))
      .eventStream(_.nonStop(fs2.Stream(1))) // suppose run forever but...
      .interruptAfter(5.seconds)
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .compile
      .toList
      .unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServicePanic])
    assert(c.isInstanceOf[ServiceStart])
    assert(d.isInstanceOf[ServicePanic])
    assert(e.isInstanceOf[ServiceStart])
    assert(f.isInstanceOf[ServicePanic])
  }

}
