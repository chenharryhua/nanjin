package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.service.{Agent, ServiceGuard}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import eu.timepit.refined.auto.*
import io.jaegertracing.Configuration
import natchez.{Span, Trace}
import natchez.jaeger.Jaeger
import natchez.log.Log
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

import java.net.URI
import scala.concurrent.duration.*

class TraceTest extends AnyFunSuite {

  val serviceGuard: ServiceGuard[IO] =
    TaskGuard[IO]("trace-guard").service("trace.service")

  // trace
  def f_unit(ag: Agent[IO])(implicit T: Trace[IO]): IO[Unit] =
    T.span("unit.trace")(T.traceId.flatMap(id => ag.trace("unit.action", id, _.notice).retry(IO(())).run))

  def f_int(ag: Agent[IO])(implicit T: Trace[IO]): IO[Int] =
    T.span("int.trace")(T.traceId.flatMap(id => ag.trace("int.action", id, _.silent).retry(IO(1)).run))

  def f_err(ag: Agent[IO])(implicit T: Trace[IO]): IO[Int] =
    T.span("err.trace")(
      T.traceId.flatMap(id =>
        ag.trace("err.action", id, _.withConstantDelay(1.seconds, 1))
          .retry(IO.raiseError[Int](new Exception("oops")))
          .run))

  test("log trace explicit") {
    implicit val log: Logger[IO] = Slf4jLogger.getLogger[IO]

    val entryPoint = Log.entryPoint[IO]("log.service")
    serviceGuard
      .eventStream(ag =>
        entryPoint
          .root("logging")
          .use(ep => Trace.ioTrace(ep).flatMap(implicit sp => f_unit(ag) >> f_int(ag) >> f_err(ag).attempt)))
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  // span

  def s_unit(ag: Agent[IO])(span: Span[IO]): IO[Unit] =
    span
      .span("unit.span")
      .use(s => s.traceId.flatMap(id => ag.trace("unit.action", id, _.notice).retry(IO(())).run))

  def s_int(ag: Agent[IO])(span: Span[IO]): IO[Int] =
    span.span("int.span").use(s => s.traceId.flatMap(id => ag.trace("int.action", id).retry(IO(1)).run))

  def s_err(ag: Agent[IO])(span: Span[IO]): IO[Int] =
    span
      .span("err.span")
      .use(s =>
        s.traceId.flatMap(id =>
          ag.trace("err.action", id, _.withConstantDelay(1.seconds, 1))
            .retry(IO.raiseError[Int](new Exception("oops")))
            .run))

  test("jaeger") {

    val entryPoint = Jaeger.entryPoint("nj.test", Some(new URI("http://localhost:16686")))(cfg =>
      IO(
        cfg
          .withSampler(Configuration.SamplerConfiguration.fromEnv.withType("const").withParam(1))
          .withReporter(Configuration.ReporterConfiguration.fromEnv.withLogSpans(true))
          .getTracer
      ))

    val run = serviceGuard.eventStream { ag =>
      entryPoint
        .flatMap(_.root("jaeger"))
        .use(span => s_unit(ag)(span) >> s_err(ag)(span).attempt >> s_int(ag)(span).delayBy(1.seconds))
    }.evalTap(console.simple[IO]).compile.drain

    (run >> IO.sleep(3.seconds)).unsafeRunSync()
  }

}
