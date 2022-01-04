package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.codahale.metrics.MetricFilter
import com.github.chenharryhua.nanjin.datetime.crontabs
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.{MetricsReport, PassThrough, ServiceStop}
import com.github.chenharryhua.nanjin.guard.observers.logging
import com.github.chenharryhua.nanjin.guard.translators.Translator
import io.circe.Decoder
import io.circe.generic.auto.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import scala.util.Random

final case class PassThroughObject(a: Int, b: String)

class PassThroughTest extends AnyFunSuite {
  val guard = TaskGuard[IO]("test").service("pass-throught")
  test("pass-through") {
    val PassThroughObject(a, b) :: rest = guard.eventStream { action =>
      List.range(0, 9).traverse(n => action.broker("pt").asError.passThrough(PassThroughObject(n, "a")))
    }.map {
      case PassThrough(_, _, _, _, v) => Decoder[PassThroughObject].decodeJson(v).toOption
      case _                          => None
    }.unNone.compile.toList.unsafeRunSync()
    assert(a == 0)
    assert(b == "a")
    assert(rest.last.a == 8)
    assert(rest.size == 8)
  }

  test("unsafe pass-through") {
    val List(PassThroughObject(a, b)) = guard.eventStream { action =>
      IO(1).map(_ => action.broker("pt").unsafePassThrough(PassThroughObject(1, "a")))
    }.debug()
      .map {
        case PassThrough(_, _, _, _, v) => Decoder[PassThroughObject].decodeJson(v).toOption
        case _                          => None
      }
      .unNone
      .compile
      .toList
      .unsafeRunSync()
    assert(a == 1)
    assert(b == "a")
  }

  test("counter") {
    val Some(last) = guard
      .updateConfig(_.withMetricReport(crontabs.secondly))
      .eventStream(ag =>
        ag.counter("counter").inc(100) >> ag.metrics.reset >> ag
          .counter("counter")
          .asError
          .inc(1)
          .delayBy(1.second)
          .replicateA(3) >> ag.metrics.fullReport)
      .debug()
      .filter(_.isInstanceOf[MetricsReport])
      .compile
      .last
      .unsafeRunSync()
    assert(last.asInstanceOf[MetricsReport].snapshot.counterMap("03.counter.[counter][0135a608].error") == 3)
  }

  test("alert") {
    val Some(last) = guard
      .updateConfig(_.withMetricReport(crontabs.c997))
      .eventStream(ag => ag.alert("oops").error("message").delayBy(1.second) >> ag.metrics.report(MetricFilter.ALL))
      .debug()
      .filter(_.isInstanceOf[MetricsReport])
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()
    assert(last.asInstanceOf[MetricsReport].snapshot.counterMap("01.alert.[oops][a32b945e].error") == 1)
  }

  test("meter") {
    guard
      .updateConfig(_.withMetricReport(1.second))
      .eventStream { agent =>
        val meter = agent.meter("nj.test.meter")
        (meter.mark(1000) >> agent.metrics.reset.whenA(Random.nextInt(3) == 1)).delayBy(1.second).replicateA(5)
      }
      .evalTap(logging(Translator.text[IO]))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("histogram") {
    guard
      .updateConfig(_.withMetricReport(1.second))
      .eventStream { agent =>
        val meter = agent.histogram("nj.test.histogram")
        (IO(Random.nextInt(100).toLong).flatMap(meter.update)).delayBy(1.second).replicateA(5)
      }
      .evalTap(logging(Translator.json[IO].map(_.noSpaces)))
      .compile
      .drain
      .unsafeRunSync()
  }
}
