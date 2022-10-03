package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import eu.timepit.refined.auto.*
import io.jaegertracing.Configuration
import natchez.jaeger.Jaeger
import natchez.log.Log
import natchez.Trace
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.net.URI

class TraceTest extends AnyFunSuite {

  test("trace") {
    implicit val log: Logger[IO] = Slf4jLogger.getLoggerFromName("test-logger")
    val logEntry                 = Log.entryPoint[IO]("logger")

    val run = TaskGuard[IO]("trace-guard")
      .withEntryPoint(logEntry)
      .service("log")
      .eventStream { ag =>
        val span = ag.root("log-root")
        val a1   = ag.action("a1").retry(IO(()))
        val a2   = ag.action("a2").retry((i: Int) => IO(i + 1))
        val a3   = ag.action("a3").retry((i: Int) => IO.raiseError(new Exception(i.toString)))

        span.use(s =>
          (a1.runWithSpan(s)) >> s
            .span("s1")
            .use(s =>
              a2.runWithSpan(1)(s) >>
                s.span("s2").use(s => a3.runWithSpan(1)(s)).attempt))
      }
      .evalMap(console.simple[IO])
      .compile
      .drain

    run.unsafeRunSync()
  }

  test("jaeger") {

    val entryPoint = Jaeger.entryPoint("nj.test", Some(new URI("http://localhost:16686")))(cfg =>
      IO(
        cfg
          .withSampler(Configuration.SamplerConfiguration.fromEnv.withType("const").withParam(1))
          .withReporter(Configuration.ReporterConfiguration.fromEnv.withLogSpans(true))
          .getTracer
      ))

    val run =
      TaskGuard[IO]("trace-guard")
        .withEntryPoint(entryPoint)
        .service("jaeger")
        .eventStream { ag =>
          val span = ag.root("jaeger-root")
          val a1   = ag.action("a1").retry(IO(()))
          val a2   = ag.action("a2").retry((i: Int) => IO(i + 1))
          val a3   = ag.action("a3").retry((i: Int) => IO.raiseError(new Exception(i.toString)))

          span
            .evalMap(Trace.ioTrace)
            .use(implicit ns =>
              (a1.runWithTrace) >>
                ns.span("cs2")(a2.runWithTrace(1)) >>
                ns.span("cs3")(a3.runWithTrace(1).attempt))
        }
        .evalTap(console.simple[IO])
        .compile
        .drain

    run.unsafeRunSync()
  }
}
