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
import org.scalatest.funsuite.AnyFunSuite

import java.net.URI
import scala.concurrent.duration.*

class TraceTest extends AnyFunSuite {

  val task = TaskGuard[IO]("trace-guard")

  // span

  def s_unit(ag: Agent[IO]) =
    ag.action(_.notice).retry(IO(()))

  def s_int(ag: Agent[IO]) =
    ag.action(_.notice).retry((i: Int) => IO(i + 1)).logOutput((_, out) => out.asJson)

  def s_err(ag: Agent[IO]) =
    ag.action(_.notice.withConstantDelay(1.seconds, 1))
      .retry((i: Int) => IO.raiseError[Int](new Exception(s"oops-$i")))

  test("trace") {

    val run = task
      .service("log")
      .eventStream { ag =>
        s_unit(ag).run("a") >> s_int(ag).run(1)("b") >> s_err(ag).run(1)("c").attempt
      }
      .evalMap(console.simple[IO])
      .compile
      .drain

    (run >> IO.sleep(3.seconds)).unsafeRunSync()
  }

  test("jaeger") {

    val entryPoint = Jaeger.entryPoint("nj.test", Some(new URI("http://localhost:16686")))(cfg =>
      IO(
        cfg
          .withSampler(Configuration.SamplerConfiguration.fromEnv.withType("const").withParam(1))
          .withReporter(Configuration.ReporterConfiguration.fromEnv.withLogSpans(true))
          .getTracer
      ))

    val run = task
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
