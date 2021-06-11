package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.alert.{LogService, MetricsService, PassThrough, SlackService}
import io.circe.Decoder
import io.circe.generic.auto._
import org.scalatest.funsuite.AnyFunSuite
import cats.syntax.all._

final case class PassThroughObject(a: Int, b: String)

class PassThroughTest extends AnyFunSuite {
  val metrics = new MetricRegistry
  val logging = SlackService(SimpleNotificationService.fake[IO]) |+| MetricsService[IO](metrics) |+| LogService[IO]

  test("pass-through") {
    val guard = TaskGuard[IO]("test").service("pass-throught")
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
}
