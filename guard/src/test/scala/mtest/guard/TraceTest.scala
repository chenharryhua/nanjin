package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.comcast.ip4s.IpLiteralSyntax
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import fs2.{Chunk, Stream}
import io.jaegertracing.Configuration
import natchez.jaeger.Jaeger
import natchez.log.Log
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry.RetryPolicies

import java.net.URI
import scala.concurrent.duration.DurationInt

class TraceTest extends AnyFunSuite {

  test("1.trace") {
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
          (a1.runInSpan(s)) >> s
            .span("s1")
            .use(s =>
              a2.runInSpan((1, 1))(s) >>
                s.span("s2").use(s => a3.runInSpan(1)(s)).attempt))
      }
      .evalMap(console.simple[IO])
      .compile
      .drain

    run.unsafeRunSync()
  }

  test("2.jaeger") {

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
            a1.runInSpan(ns) >>
              a2.runInSpan((1, 2))(ns) >>
              a3.runInSpan(1)(ns).attempt)
        }
        .evalTap(console.verboseJson[IO])
        .compile
        .drain

    run.unsafeRunSync()
  }

// nc -kluvw 0 127.0.0.1 1026
  test("3.udp_test") {
    TaskGuard[IO]("udp_test")
      .service("udp_test")
      .eventStream { agent =>
        agent
          .ticks(RetryPolicies.constantDelay[IO](1.second).join(RetryPolicies.limitRetries(3)))
          .flatMap { _ =>
            Stream.resource(
              agent.udpClient("udp_test").withHistogram.withCounting.socket(ip"127.0.0.1", port"1026"))
          }
          .evalTap(_.write(Chunk.indexedSeq("abcdefghijklmnopqrstuvwxyz\n".getBytes())))
          .compile
          .drain >> agent.metrics.report
      }
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }
}
