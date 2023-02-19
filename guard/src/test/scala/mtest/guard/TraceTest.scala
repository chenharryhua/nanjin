package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import eu.timepit.refined.auto.*
import io.jaegertracing.Configuration
import natchez.jaeger.Jaeger
import natchez.log.Log
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
        val a1   = ag.action("a1").retry(unit_fun)
        val a2   = ag.action("a2").retry(add_fun _)
        val a3   = ag.action("a3").retry(err_fun _)

        span.use(s =>
          (a1.runWithSpan(s)) >> s
            .span("s1")
            .use(s =>
              a2.runWithSpan((1, 1))(s) >>
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
          val a1   = ag.action("a1", _.notice).retry(unit_fun)
          val a2   = ag.action("a2", _.notice).retry(add_fun _)
          val a3   = ag.action("a3", _.notice).retry(err_fun _)

          span.use(ns =>
            a1.runWithSpan(ns) >>
              a2.runWithSpan((1, 2))(ns) >>
              a3.runWithSpan(1)(ns).attempt)
        }
        .evalTap(console.json[IO])
        .compile
        .drain

    run.unsafeRunSync()
  }
}
