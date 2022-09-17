package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import eu.timepit.refined.auto.*
import io.circe.syntax.*
import io.jaegertracing.Configuration.{ReporterConfiguration, SamplerConfiguration}
import natchez.{Trace, TraceValue}
import natchez.jaeger.Jaeger
import natchez.log.Log
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.*

class TraceTest extends AnyFunSuite {
  implicit val log: Logger[IO] = Slf4jLogger.getLogger[IO]
  val serviceGuard: ServiceGuard[IO] =
    TaskGuard[IO]("trace-guard").service("trace test")

  val entryPoint = Log.entryPoint[IO]("log.service")

  test("log trace explicit") {
    serviceGuard.eventStream { ag =>
      entryPoint.root("root").use { root =>
        ag.action("add-one").critical.retry((a: Int) => IO(a + 1)).logInput(_.asJson).trace(root)(1) >>
          root.span("child_span_1").use { span2 =>
            span2.put(("a", TraceValue.BooleanValue(true))) >>
              ag.action("add-two").critical.retry((a: Int) => IO(a + 2)).trace(span2)(1)
          } >> root
            .span("child_span_2")
            .use(span2 =>
              span2.put(("c", TraceValue.StringValue("c"))) >>
                ag.action("do nothing").retry(IO(0)).trace(span2))
      }
    }.evalTap(console.simple[IO]).compile.drain.unsafeRunSync()
  }

  test("log trace implicit") {
    serviceGuard.eventStream { ag =>
      entryPoint.root("root").use { root =>
        Trace.ioTrace(root).flatMap { implicit trace =>
          ag.action("add-one").critical.retry((a: Int) => IO(a + 1)).logInput(_.asJson).trace(1) >>
            root.span("child_span_1").use { span2 =>
              span2.put(("a", TraceValue.BooleanValue(true))) >>
                ag.action("constant").critical.retry(IO(2)).trace
            } >> root
              .span("child_span_2")
              .use { implicit span2 =>
                span2.put(("c", TraceValue.StringValue("c"))) >>
                  ag.action("exception")
                    .updateConfig(_.withConstantDelay(1.seconds, 3))
                    .retry(IO.raiseError(new Exception("abc"))) // fail when 'new Exception'
                    .trace
              }
              .attempt
        }
      }
    }.evalTap(console.simple[IO]).compile.drain.unsafeRunSync()
  }

  test("jaeger") {
    val entryPoint = Jaeger.entryPoint("nj.test")(cfg =>
      IO(cfg.withSampler(SamplerConfiguration.fromEnv).withReporter(ReporterConfiguration.fromEnv).getTracer))

    def plusOne(a: Int)(implicit t: Trace[IO]) = t.span("plus.one")(t.put(("a", 1)) >> IO(a + 1))
    def plusTwo(a: Int)(implicit t: Trace[IO]) = t.span("plus.two")(t.put(("b", 2)) >> IO(a + 2))
    //  def plusThree(a: Int)(implicit t: Trace[IO]) = t.span("plus.three")(IO(a + 3))

    serviceGuard.eventStream { ag =>
      entryPoint.use(_.root("nj.entry").evalMap(Trace.ioTrace).use { implicit root =>
        ag.action("add-one").notice.retry(plusOne(1)).trace >>
          ag.action("add-two").notice.retry(plusTwo(2)).trace
      })
    }.evalTap(console.simple[IO]).compile.drain.unsafeRunSync()
  }
}
