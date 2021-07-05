package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.alert.{
  ForYourInformation,
  LogService,
  MetricsService,
  NJEvent,
  PassThrough,
  ServiceStopped,
  SlackService
}
import io.circe.Decoder
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.syntax._
import org.scalatest.funsuite.AnyFunSuite

final case class PassThroughObject(a: Int, b: String)

class PassThroughTest extends AnyFunSuite {
  val metrics = new MetricRegistry
  val logging = SlackService(SimpleNotificationService.fake[IO]) |+| MetricsService[IO](metrics) |+| LogService[IO]
  val guard   = TaskGuard[IO]("test").service("pass-throught")
  test("pass-through") {
    val List(PassThroughObject(a, b)) = guard.eventStream { action =>
      action("send-json").passThrough(PassThroughObject(1, "a"))
    }.observe(_.evalMap(m => logging.alert(m)).drain)
      .map {
        case PassThrough(_, v) => Decoder[PassThroughObject].decodeJson(v).toOption
        case _                 => None
      }
      .unNone
      .compile
      .toList
      .unsafeRunSync()
    assert(a == 1)
    assert(b == "a")
  }

  test("unsafe pass-through") {
    val List(PassThroughObject(a, b)) = guard.eventStream { action =>
      IO(1).map(_ => action("send-json").unsafePassThrough(PassThroughObject(1, "a")))
    }.observe(_.evalMap(m => logging.alert(m)).drain)
      .map {
        case PassThrough(_, v) => Decoder[PassThroughObject].decodeJson(v).toOption
        case _                 => None
      }
      .unNone
      .compile
      .toList
      .unsafeRunSync()
    assert(a == 1)
    assert(b == "a")
  }

  test("for your information") {
    val Vector(a, b) = guard
      .eventStream(_.fyi("hello, world"))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .observe(_.evalMap(m => logging.alert(m)).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(!a.asInstanceOf[ForYourInformation].isError)
    assert(b.isInstanceOf[ServiceStopped])
  }

  test("unsafe FYI") {
    val Vector(a, b) = guard
      .eventStream(ag => IO(1).map(_ => ag.unsafeFYI("hello, world")))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .observe(_.evalMap(m => logging.alert(m)).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(!a.asInstanceOf[ForYourInformation].isError)
    assert(b.isInstanceOf[ServiceStopped])
  }

  test("report error") {
    val Vector(a, b) = guard
      .eventStream(_.reportError("error"))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .observe(_.evalMap(m => logging.alert(m)).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.asInstanceOf[ForYourInformation].isError)
    assert(b.isInstanceOf[ServiceStopped])
  }

  test("unsafe report error") {
    val Vector(a, b) = guard
      .eventStream(ag => IO(1).map(_ => ag.unsafeReportError("error")))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .observe(_.evalMap(m => logging.alert(m)).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.asInstanceOf[ForYourInformation].isError)
    assert(b.isInstanceOf[ServiceStopped])
  }
}
