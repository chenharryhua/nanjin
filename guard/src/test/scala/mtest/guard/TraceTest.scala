package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.service.{Agent, ServiceGuard}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class TraceTest extends AnyFunSuite {

  val serviceGuard: ServiceGuard[IO] =
    TaskGuard[IO]("trace-guard").service("trace.service")

  // span

  def s_unit(ag: Agent[IO]): IO[Unit] =
    ag.action(_.notice).retry(IO(())).run("unit.action")

  def s_int(ag: Agent[IO]): IO[Int] =
    ag.action(_.notice).retry(IO(1)).run("int.action")

  def s_err(ag: Agent[IO]): IO[Int] =
    ag.action(_.notice.withConstantDelay(1.seconds, 1))
      .retry(IO.raiseError[Int](new Exception("oops")))
      .run("err.action")

  test("trace") {

    val run = serviceGuard.eventStream { ag =>
      s_unit(ag) >> s_int(ag) >> s_err(ag).attempt
    }.evalMap(console.simple[IO]).compile.drain

    (run >> IO.sleep(3.seconds)).unsafeRunSync()
  }

}
