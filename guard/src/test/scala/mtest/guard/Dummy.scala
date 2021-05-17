package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.regions.Regions
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.guard.{AlertService, TaskGuard}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

class Dummy extends AnyFunSuite {
  test("on cancell") {
    val run = for {
      a <- IO.println("hello").delayBy(3.seconds).onCancel(IO(println("cancelled"))).start
      _ <- IO.println("world")
      _ <- IO(1 / 0).onError(_ => a.cancel)
      _ <- a.join
    } yield ()
    // (run.attempt >> IO.sleep(5.seconds)).unsafeRunSync()
  }

}
