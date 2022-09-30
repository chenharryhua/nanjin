package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.service.{Agent, ServiceGuard}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class TraceTest extends AnyFunSuite {

  val serviceGuard: ServiceGuard[IO] =
    TaskGuard[IO]("trace-guard").service("trace.service")

  // span

  def s_unit(ag: Agent[IO]): IO[Unit] =
    ag.trace("unit.action", "1", _.notice).use(_.retry(IO(())).run)

  def s_int(ag: Agent[IO]): IO[Int] =
    ag.trace("int.action", "2", _.notice).use(_.retry(IO(1)).run)

  def s_err(ag: Agent[IO]): IO[Int] =
    ag.trace("err.action", "3", _.notice.withConstantDelay(1.seconds, 1))
      .use(_.retry(IO.raiseError[Int](new Exception("oops"))).run)

  test("trace") {

    val run = serviceGuard.eventStream { ag =>
      s_unit(ag) >> s_err(ag).attempt >> s_int(ag).delayBy(1.seconds)
    }.debug().compile.drain

    (run >> IO.sleep(3.seconds)).unsafeRunSync()
  }

}
