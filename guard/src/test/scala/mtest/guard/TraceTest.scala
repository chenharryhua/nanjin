package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.service.{Agent, ServiceGuard}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import eu.timepit.refined.auto.*
import io.jaegertracing.Configuration.{ReporterConfiguration, SamplerConfiguration}
import natchez.log.Log
import natchez.Trace
import natchez.jaeger.Jaeger
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

import java.net.URI
import scala.concurrent.duration.*

class TraceTest extends AnyFunSuite {
  implicit val log: Logger[IO] = Slf4jLogger.getLogger[IO]

  val serviceGuard: ServiceGuard[IO] =
    TaskGuard[IO]("trace-guard").service("trace.service")

  def f_unit(ag: Agent[IO])(implicit T: Trace[IO]): IO[Unit] =
    T.span("unit.span")(ag.action("unit.action").notice.retry(IO(())).runTrace)

  def f_int(ag: Agent[IO])(implicit T: Trace[IO]): IO[Int] =
    T.span("int.span")(ag.action("int.action").retry(IO(1)).runTrace)

  def f_err(ag: Agent[IO])(implicit T: Trace[IO]): IO[Int] =
    T.span("err.span")(
      ag.action("err.action")
        .updateConfig(_.withConstantDelay(1.seconds, 1))
        .retry(IO.raiseError[Int](new Exception("oops")))
        .runTrace)

  test("log trace explicit") {
    val entryPoint = Log.entryPoint[IO]("log.service")
    serviceGuard
      .eventStream(ag =>
        entryPoint
          .root("root")
          .use(ep => Trace.ioTrace(ep).flatMap(implicit sp => f_unit(ag) >> f_int(ag) >> f_err(ag).attempt)))
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("jaeger") {
    val entryPoint = Jaeger.entryPoint("nj.test", Some(new URI("http://localhost:16686")))(cfg =>
      IO(cfg.withSampler(SamplerConfiguration.fromEnv).withReporter(ReporterConfiguration.fromEnv).getTracer))

    val run = serviceGuard.eventStream { ag =>
      entryPoint.use(_.root("root").use(ep =>
        Trace.ioTrace(ep).flatMap(implicit sp => f_unit(ag) >> f_int(ag) >> f_err(ag).attempt)))
    }.evalTap(console.simple[IO]).compile.drain

    (run >> IO.sleep(3.seconds)).unsafeRunSync()
  }
}
