package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.crontabs
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.{MetricsReport, PassThrough}
import io.circe.Decoder
import io.circe.generic.auto.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

final case class PassThroughObject(a: Int, b: String)

class PassThroughTest extends AnyFunSuite {
  val guard = TaskGuard[IO]("test").service("pass-throught")
  test("pass-through") {
    val PassThroughObject(a, b) :: rest = guard.eventStream { action =>
      val pt = action("pass-though-json").notice
      List.range(0, 9).traverse(n => pt.passThrough(PassThroughObject(n, "a")))
    }.map {
      case PassThrough(_, _, v) => Decoder[PassThroughObject].decodeJson(v).toOption
      case _                    => None
    }.unNone.compile.toList.unsafeRunSync()
    assert(a == 0)
    assert(b == "a")
    assert(rest.last.a == 8)
    assert(rest.size == 8)
  }

  test("unsafe pass-through") {
    val List(PassThroughObject(a, b)) = guard.eventStream { action =>
      IO(1).map(_ => action("passthrough-json-unsafe").notice.unsafePassThrough(PassThroughObject(1, "a")))
    }.map {
      case PassThrough(_, _, v) => Decoder[PassThroughObject].decodeJson(v).toOption
      case _                    => None
    }.unNone.compile.toList.unsafeRunSync()
    assert(a == 1)
    assert(b == "a")
  }

  test("counter") {
    val Some(last) = guard
      .updateConfig(_.withMetricSchedule(crontabs.secondly))
      .eventStream(action => action("counter").count(1).delayBy(1.second).foreverM)
      .interruptAfter(5.seconds)
      .compile
      .last
      .unsafeRunSync()
    assert(last.asInstanceOf[MetricsReport].snapshot.counters("11.counter.[counter]") > 3)
  }
}
