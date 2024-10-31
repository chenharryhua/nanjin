package mtest.guard

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.Policy.*
import com.github.chenharryhua.nanjin.common.chrono.zones.berlinTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.translator.*
import io.circe.Json
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

}
