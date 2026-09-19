package mtest.guard

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.zones.berlinTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import munit.CatsEffectSuite

import scala.concurrent.duration.DurationInt

class ConfigTest extends CatsEffectSuite {
  val task: TaskGuard[IO] =
    TaskGuard[IO]("config")
      .updateConfig(_.withZoneId(berlinTime))
      .updateConfig(_.withReportPolicy(_.crontab(_.hourly).repeat))

  test("1.tick") {
    TaskGuard[IO]("tick")
      .service("tick")
      .eventStreamS(_.tickFuture(_.fixedDelay(1.seconds).repeat.limited(5)))
      .compile
      .drain
  }

  test("2.withZoneId builder function") {
    TaskGuard[IO]("zone")
      .updateConfig(_.withZoneId(_.sydneyTime))
      .service("zone")
      .eventStream(_ => IO.unit)
      .compile
      .toList
      .map { events =>
        assert(events.head.serviceIdentity.launchTime.zoneId.getId == "Australia/Sydney")
      }
  }

}
