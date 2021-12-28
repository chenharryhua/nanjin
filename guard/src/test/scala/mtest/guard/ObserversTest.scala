package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.aws.{ses, sns}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.{email, logging, slack}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.util.Random
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.crontabs

class ObserversTest extends AnyFunSuite {

  test("logging") {
    TaskGuard[IO]("logging")
      .service("text")
      .withBrief("about")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(crontabs.hourly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("logging").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag.run(IO.raiseError(new Exception("oops"))).attempt
      }
      .evalTap(
        logging
          .text[IO]
          .updateTranslator(
            _.withServiceStarted(_ => "SVC started")
              .withActionStart(_ => IO("Action up"))
              .withActionRetrying(_ => IO(Some("Retrying")))
              .withActionFailed(_ => Some("failed"))
              .withActionSucced(_ => "succ")))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("slack") {
    TaskGuard[IO]("sns")
      .service("slack")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(crontabs.secondly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("slack").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag.run(IO.raiseError(new Exception("oops"))).attempt
      }
      .evalTap(logging.text[IO])
      .through(slack[IO](sns.fake[IO]).at("@chenh"))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("mail") {
    val mail =
      email[IO]("from", List("to"), "subjct", ses.fake[IO])
        .withInterval(5.seconds)
        .withChunkSize(100)
        .withLogging
        .updateTranslator(_.skipActionStart)

    TaskGuard[IO]("ses")
      .service("email")
      .updateConfig(_.withMetricReport(1.second).withConstantDelay(1.second))
      .eventStream(
        _.span("mail")
          .max(3)
          .updateConfig(_.withConstantDelay(1.second))
          .run(IO.raiseError(new Exception).whenA(Random.nextBoolean()))
          .foreverM)
      .interruptAfter(15.seconds)
      .through(mail)
      .compile
      .drain
      .unsafeRunSync()
  }
}
