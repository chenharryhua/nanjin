package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ServiceMessage, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.observers.console
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class ActionTest extends AnyFunSuite {

  test("null") {
    val List(a, b, c, d) = TaskGuard[IO]("logError")
      .service("no exception")
      .eventStream(_.facilitate("exception", _.withPolicy(Policy.fixedDelay(1.seconds).limited(2))) {
        _.action(IO.raiseError[Int](new Exception)).buildWith(_.tapError((_, _, _) =>
          null.asInstanceOf[String].asJson))
      }.use(_.run(())))
      .map(checkJson)
      .evalTap(console.text[IO])
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceStop])
  }
}
