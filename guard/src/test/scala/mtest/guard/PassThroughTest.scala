package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.codahale.metrics.MetricFilter
import com.github.chenharryhua.nanjin.datetime.crontabs
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.observers.logging
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import com.github.chenharryhua.nanjin.guard.translators.Translator
import eu.timepit.refined.auto.*
import io.circe.Decoder
import io.circe.generic.auto.*
import io.circe.parser.decode
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import scala.util.Random
import io.circe.syntax.*

final case class PassThroughObject(a: Int, b: String)

class PassThroughTest extends AnyFunSuite {
  val guard: ServiceGuard[IO] = TaskGuard[IO]("test").service("pass-throught")
  test("1.pass-through") {
    val PassThroughObject(a, b) :: rest = guard.eventStream { action =>
      List.range(0, 9).traverse(n => action.broker("pt").asError.passThrough(PassThroughObject(n, "a")))
    }.map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .map {
        case PassThrough(_, _, _, _, v) => Decoder[PassThroughObject].decodeJson(v).toOption
        case _                          => None
      }
      .unNone
      .compile
      .toList
      .unsafeRunSync()
    assert(a == 0)
    assert(b == "a")
    assert(rest.last.a == 8)
    assert(rest.size == 8)
  }

  test("2.counter") {
    val Some(last) = guard
      .updateConfig(_.withMetricReport(crontabs.secondly))
      .eventStream(ag =>
        ag.counter("counter").inc(100) >> ag.metrics.reset >> ag
          .span("one")
          .span("two")
          .span("three")
          .counter("counter")
          .asError
          .inc(1)
          .delayBy(1.second)
          .replicateA(3) >> ag.metrics.fullReport)
      .filter(_.isInstanceOf[MetricReport])
      .debug()
      .compile
      .last
      .unsafeRunSync()
    assert(
      last
        .asInstanceOf[MetricReport]
        .snapshot
        .counterMap("03.counter.[one/two/three/counter][1a8af341].error") == 3)
  }

  test("3.alert") {
    val Some(last) = guard
      .updateConfig(_.withMetricReport(crontabs.c997))
      .eventStream(ag =>
        ag.alert("oops").withCounting.error("message").delayBy(1.second) >> ag.metrics.report(
          MetricFilter.ALL))
      .filter(_.isInstanceOf[MetricReport])
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()
    assert(last.asInstanceOf[MetricReport].snapshot.counterMap("01.alert.[oops][a32b945e].error") == 1)
  }

  test("4.meter") {
    guard
      .updateConfig(_.withMetricReport(1.second))
      .eventStream { agent =>
        val meter = agent.meter("nj.test.meter")
        (meter.mark(1000) >> agent.metrics.reset
          .whenA(Random.nextInt(3) == 1)).delayBy(1.second).replicateA(5)
      }
      .evalTap(logging(Translator.simpleText[IO]))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("5.histogram") {
    guard
      .updateConfig(_.withMetricReport(1.second))
      .eventStream { agent =>
        val meter = agent.histogram("nj.test.histogram")
        IO(Random.nextInt(100).toLong).flatMap(meter.update).delayBy(1.second).replicateA(5)
      }
      .evalTap(logging(Translator.simpleText[IO]))
      .compile
      .drain
      .unsafeRunSync()
  }
}
