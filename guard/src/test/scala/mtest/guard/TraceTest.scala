package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import eu.timepit.refined.auto.*
import io.jaegertracing.Configuration.{ReporterConfiguration, SamplerConfiguration}
import natchez.jaeger.Jaeger
import natchez.log.Log
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.net.URI
import scala.concurrent.duration.*

class TraceTest extends AnyFunSuite {
  implicit val log: Logger[IO] = Slf4jLogger.getLogger[IO]
  val entryPoint               = Log.entryPoint[IO]("log.service")
  val serviceGuard: ServiceGuard[IO] =
    TaskGuard[IO]("trace-guard").withEntryPoint(entryPoint).service("trace test")

  test("log trace explicit") {
    serviceGuard.eventStream { ag =>
      ag.root("root").use(_.notice.run(IO(1)))
    }.evalTap(console.simple[IO]).compile.drain.unsafeRunSync()
  }

  test("jaeger") {
    val entryPoint = Jaeger.entryPoint("nj.test", Some(new URI("http://localhost:16686")))(cfg =>
      IO(cfg.withSampler(SamplerConfiguration.fromEnv).withReporter(ReporterConfiguration.fromEnv).getTracer))

    val run = serviceGuard
      .withEntryPoint(entryPoint)
      .eventStream { ag =>
        ag.root("nj.jaeger.test").use { sp1 =>
          sp1.critical.run(IO(1)) >>
            sp1.span("children-1").use(sp2 => sp2.critical.run(IO(2))) >>
            sp1
              .span("children-2")
              .use(_.critical
                .updateConfig(_.withConstantDelay(1.seconds, 3))
                .run(IO.raiseError(new Exception("oops"))))
              .attempt
        }
      }
      .evalTap(console.simple[IO])
      .compile
      .drain

    (run >> IO.sleep(3.seconds)).unsafeRunSync()
  }
}
