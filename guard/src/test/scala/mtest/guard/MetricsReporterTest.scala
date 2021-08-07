package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.alert.NJConsoleReporter
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class MetricsReporterTest extends AnyFunSuite {
  test("metrics reporter") {
    TaskGuard[IO]("task")
      .service("service")
      .withReporter(NJConsoleReporter(2.second))
      //.withReporter(NJSlf4jReporter(2.seconds))
      .eventStream(ag => ag.run(IO.println("running").delayBy(1.second)).foreverM)
      .interruptAfter(10.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }
}
