package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStart
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

class AddBriefTest extends AnyFunSuite {

  private val guard = TaskGuard[IO]("brief")

  test("1.single addBrief appears in service params") {
    val events = guard
      .service("single")
      .updateConfig(_.addBrief("hello"))
      .eventStream(_ => IO.unit)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val brief = events.head.asInstanceOf[ServiceStart].serviceParams.brief.value
    val arr = brief.asArray.get
    assert(arr.contains(Json.fromString("hello")))
  }

  test("2.multiple addBrief calls accumulate") {
    val events = guard
      .service("multi")
      .updateConfig(_.addBrief("first").addBrief("second").addBrief("third"))
      .eventStream(_ => IO.unit)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val brief = events.head.asInstanceOf[ServiceStart].serviceParams.brief.value
    val arr = brief.asArray.get
    assert(arr.contains(Json.fromString("first")))
    assert(arr.contains(Json.fromString("second")))
    assert(arr.contains(Json.fromString("third")))
    assert(arr.size == 3)
  }

  test("3.null briefs are filtered out") {
    val events = guard
      .service("nulls")
      .updateConfig(_.addBrief(Json.Null).addBrief("keep").addBrief(Json.Null))
      .eventStream(_ => IO.unit)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val brief = events.head.asInstanceOf[ServiceStart].serviceParams.brief.value
    val arr = brief.asArray.get
    // Nulls should be filtered out
    assert(!arr.contains(Json.Null))
    assert(arr.contains(Json.fromString("keep")))
    assert(arr.size == 1)
  }

  test("4.duplicate briefs are deduplicated") {
    val events = guard
      .service("dedup")
      .updateConfig(_.addBrief("same").addBrief("same").addBrief("different"))
      .eventStream(_ => IO.unit)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val brief = events.head.asInstanceOf[ServiceStart].serviceParams.brief.value
    val arr = brief.asArray.get
    // "same" should appear only once due to .distinct
    assert(arr.count(_ == Json.fromString("same")) == 1)
    assert(arr.contains(Json.fromString("different")))
    assert(arr.size == 2)
  }

  test("5.addBrief with complex JSON object") {
    case class Meta(version: String, env: String)
    given io.circe.Encoder[Meta] = io.circe.Encoder.forProduct2("version", "env")(m => (m.version, m.env))

    val events = guard
      .service("complex")
      .updateConfig(_.addBrief(Meta("1.0.0", "prod")))
      .eventStream(_ => IO.unit)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val brief = events.head.asInstanceOf[ServiceStart].serviceParams.brief.value
    val arr = brief.asArray.get
    val meta = arr.head
    assert(meta.hcursor.get[String]("version").toOption.contains("1.0.0"))
    assert(meta.hcursor.get[String]("env").toOption.contains("prod"))
  }

  test("6.no addBrief produces empty brief array") {
    val events = guard
      .service("empty")
      .eventStream(_ => IO.unit)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val brief = events.head.asInstanceOf[ServiceStart].serviceParams.brief.value
    val arr = brief.asArray.get
    assert(arr.isEmpty)
  }
}
