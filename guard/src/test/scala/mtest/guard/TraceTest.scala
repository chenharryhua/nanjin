package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import eu.timepit.refined.auto.*
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
    ag.action(_.notice).retry(IO(1))

  def s_err(ag: Agent[IO]) =
    ag.action(_.notice.withConstantDelay(1.seconds, 1)).retry(IO.raiseError[Int](new Exception("oops")))

  test("trace") {

    val run = task
      .service("log")
      .eventStream { ag =>
        s_unit(ag).run("a") >> s_int(ag).run("b") >> s_err(ag).run("c").attempt
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
            ns.span("child1").use(_.run(s_unit(ag))) >>
              ns.span("child2").use(_.run(s_err(ag))).attempt >>
              ns.span("child3").use(_.run(s_int(ag))))
      }
      .evalTap(console.simple[IO])
      .compile
      .drain

    run.unsafeRunSync()
  }

}
