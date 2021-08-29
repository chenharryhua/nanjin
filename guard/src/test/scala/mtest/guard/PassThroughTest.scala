package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.PassThrough
import io.circe.Decoder
import io.circe.generic.auto.*
import org.scalatest.funsuite.AnyFunSuite

final case class PassThroughObject(a: Int, b: String)

class PassThroughTest extends AnyFunSuite {
  val guard = TaskGuard[IO]("test").service("pass-throught")
  test("pass-through") {
    val PassThroughObject(a, b) :: rest = guard.eventStream { action =>
      val pt = action("pass-though-json").notice
      List.fill(10)(1).traverse(n => pt.passThrough(PassThroughObject(n, "a")))
    }.map {
      case PassThrough(_, _, v) => Decoder[PassThroughObject].decodeJson(v).toOption
      case _                    => None
    }.unNone.compile.toList.unsafeRunSync()
    assert(a == 1)
    assert(b == "a")
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
    guard.eventStream(action => action("counter").count(1)).compile.drain.unsafeRunSync()

  }

}
