package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.alert.PassThrough
import io.circe.Decoder
import io.circe.generic.auto._
import org.scalatest.funsuite.AnyFunSuite

final case class PassThroughObject(a: Int, b: String)

class PassThroughTest extends AnyFunSuite {
  test("pass-through") {
    val guard = TaskGuard[IO]("test").service("pass-throught")
    val List(PassThroughObject(a, b)) = guard.eventStream { action =>
      action("send-json").passThrough(PassThroughObject(1, "a"))
    }.map {
      case PassThrough(_, v) => Decoder[PassThroughObject].decodeJson(v).toOption
      case _                 => None
    }.unNone.compile.toList.unsafeRunSync()
    assert(a == 1)
    assert(b == "a")
  }
}
