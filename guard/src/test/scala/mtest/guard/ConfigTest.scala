package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.chrono.Policy.*
import com.github.chenharryhua.nanjin.common.chrono.zones.berlinTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.AlarmLevel
import com.github.chenharryhua.nanjin.guard.translator.*
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class ConfigTest extends AnyFunSuite {
  val task: TaskGuard[IO] =
    TaskGuard[IO]("config")
      .updateConfig(
        _.withZoneId(berlinTime)
          .withPanicHistoryCapacity(1)
          .withMetricHistoryCapacity(2)
          .withErrorHistoryCapacity(3))
      .updateConfig(_.withMetricReport(crontab(_.hourly)))
      .updateConfig(_.withJmx(identity))
      .updateConfig(_.withAlarmLevel(_.Info))
      .updateConfig(_.withAlarmLevel(AlarmLevel.Info))
      .updateConfig(_.withTaskName("conf"))

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

  test("tick") {
    TaskGuard[IO]("tick")
      .service("tick")
      .eventStreamS(_.tickImmediately(Policy.fixedDelay(1.seconds).limited(5)).debug())
      .compile
      .drain
      .unsafeRunSync()
  }

}
