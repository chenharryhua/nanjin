package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.berlinTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
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
      .updateConfig(_.withMetricReport(_.crontab(_.hourly)))
      .updateConfig(_.withJmx(identity))
      .updateConfig(_.withTaskName("conf"))

  test("tick") {
    TaskGuard[IO]("tick")
      .service("tick")
      .eventStreamS(_.tickFuture(_.fixedDelay(1.seconds).limited(5)).debug())
      .compile
      .drain
      .unsafeRunSync()
  }

}
