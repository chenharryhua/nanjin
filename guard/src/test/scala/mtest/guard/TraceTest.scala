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
    ag.action("unit.action", _.notice).retry(IO(())).run

  def s_int(ag: Agent[IO]): IO[Int] =
    ag.action("int.action", _.notice).retry(IO(1)).run

  def s_err(ag: Agent[IO]): IO[Int] =
    ag.action("err.action", _.notice.withConstantDelay(1.seconds, 1))
      .retry(IO.raiseError[Int](new Exception("oops")))
      .run

  test("trace") {

    val run = serviceGuard.eventStream { ag =>
      s_unit(ag) >> s_int(ag) >> s_err(ag).attempt
    }.evalMap(console.simple[IO]).compile.drain

    (run >> IO.sleep(3.seconds)).unsafeRunSync()
  }

}
