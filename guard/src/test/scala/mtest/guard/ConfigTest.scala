package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.Policy.*
import com.github.chenharryhua.nanjin.common.chrono.zones.berlinTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.translator.*
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

class ConfigTest extends AnyFunSuite {
  val task: TaskGuard[IO] =
    TaskGuard[IO]("config")
      .updateConfig(_.withZoneId(berlinTime).withPanicHistoryCapacity(1).withMetricHistoryCapacity(2))
      .updateConfig(_.withMetricReport(crontab(_.hourly)))

  test("9.case") {
    val en = EventName.ServiceStart
    assert(en.entryName == "Service Start")
    assert(en.snake == "service_start")
    assert(en.compact == "ServiceStart")
    assert(en.camel == "serviceStart")
    assert(en.camelJson == Json.fromString("serviceStart"))
    assert(en.snakeJson == Json.fromString("service_start"))
    assert(en.compactJson == Json.fromString("ServiceStart"))
  }

  test("10.brief merge") {
    import io.circe.generic.auto.*
    final case class A(a: Int, z: Int)
    final case class B(b: Int, z: String)

    val ss = task
      .service("brief merge")
      .updateConfig(_.addBrief(A(1, 3).asJson))
      .updateConfig(_.addBrief(B(2, "b")))
      .updateConfig(_.addBrief(Json.Null))
      .updateConfig(_.addBrief(IO(A(1, 3))))
      .eventStream(_.facilitate("cfg")(_.metrics.meter("m")).use_)
      .map(checkJson)
      .filter(_.isInstanceOf[ServiceStart])
      .compile
      .last
      .unsafeRunSync()
      .get
      .asInstanceOf[ServiceStart]
    val ab = ss.serviceParams.brief.noSpaces
    assert(ab === """[{"a":1,"z":3},{"b":2,"z":"b"}]""")
  }
}
