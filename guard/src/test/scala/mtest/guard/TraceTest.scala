package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import eu.timepit.refined.auto.*
import io.circe.syntax.EncoderOps
import io.jaegertracing.Configuration
import natchez.jaeger.Jaeger
import natchez.log.Log
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.net.URI
import scala.concurrent.duration.*

class TraceTest extends AnyFunSuite {

  def s_unit(ag: Agent[IO]) =
    ag.action(_.notice).retry(IO(()))

  def s_int(ag: Agent[IO]) =
    ag.action(_.notice).retry((i: Int) => IO(i + 1)).logOutput((_, out) => out.asJson)

  def s_err(ag: Agent[IO]) =
    ag.action(_.notice.withConstantDelay(1.seconds, 1))
      .retry((i: Int) => IO.raiseError[Int](new Exception(s"oops-$i")))

  test("trace") {
    implicit val log: Logger[IO] = Slf4jLogger.getLoggerFromName("test-logger")
    val logEntry                 = Log.entryPoint[IO]("logger")

    val run = TaskGuard[IO]("trace-guard")
      .withEntryPoint(logEntry)
      .service("log")
      .eventStream { ag =>
        val span = ag.root("log-root")
        val a1   = ag.action(_.silent).retry(IO(()))
        val a2   = ag.action(_.silent).retry((i: Int) => IO(i + 1))
        val a3   = ag.action(_.silent).retry((i: Int) => IO.raiseError(new Exception(i.toString)))

        span.use(s =>
          s.runAction(a1) >> s
            .span("c1")
            .use(s =>
              s.runAction(a2)(1) >>
                s.span("c2").use(_.runAction(a3)(1)).attempt))
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
          ag.root("jaeger-root2")
            .use(ns =>
              ns.put("a" -> "a") >>
                ns.span("child1").use(_.runAction(s_unit(ag))) >>
                ns.span("grandchild").use { ns =>
                  ns.put("g1" -> "g1") >>
                    ns.span("g1")
                      .use(ns => ns.put("e1" -> "e1") >> ns.runAction(s_err(ag))(1) >> ns.put("e2" -> "e2"))
                      .attempt >>
                    ns.span("g2").use(ns => ns.runAction(s_int(ag))(1).flatMap(r => ns.put("result" -> r)))
                })
        }
        .evalTap(console.simple[IO])
        .compile
        .drain

    run.unsafeRunSync()
  }
}
