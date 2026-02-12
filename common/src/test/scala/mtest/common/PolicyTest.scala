package mtest.common

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class PolicyTest extends AnyFunSuite {

  test("policy") {
    val policy =
      Policy.crontab(_.every5Minutes).jitter(30.seconds)

    tickStream.testPolicy[IO](policy).take(3).debug().compile.toList.unsafeRunSync()
  }

}
