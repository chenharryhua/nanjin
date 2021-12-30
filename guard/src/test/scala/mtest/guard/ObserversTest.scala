package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.aws.{ses, sns}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.{console, email, logging, slack}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.util.Random
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.crontabs
import io.circe.Json

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

  test("console - text") {
    TaskGuard[IO]("console")
      .service("text")
      .withBrief("about console")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(crontabs.secondly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("console").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag.run(IO.raiseError(new Exception("oops"))).attempt
      }
      .evalTap(
        console
          .text[IO]
          .updateTranslator(
            _.withServiceStarted(_ => "SVC started")
              .withActionStart(_ => IO("Action up"))
              .withActionRetrying(_ => IO(Some("Retrying")))
              .withActionFailed(_ => Some("failed"))
              .withActionSucced(_ => "succ")
              .skipMetricsReport
              .skipServiceStopped))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("console - json") {
    TaskGuard[IO]("console")
      .service("json")
      .withBrief("about console")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(crontabs.secondly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("console").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag.run(IO.raiseError(new Exception("oops"))).attempt
      }
      .evalTap(console
        .json[IO](_.spaces2)
        .updateTranslator(_.withServiceStarted(_ => Json.fromString("service was kicked off")).skipServiceStopped))
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
      .through(slack[IO](sns.fake[IO])(_.at("chenh").withLogging).updateTranslator(_.skipActionFailed))
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
      .updateConfig(_.withMetricReport(100.second).withConstantDelay(1.second))
      .eventStream(
        _.span("mail")
          .max(3)
          .updateConfig(_.withConstantDelay(1.second))
          .notice
          .run(IO.raiseError(new Exception).whenA(Random.nextBoolean()))
          .delayBy(2.seconds)
          .foreverM)
      .interruptAfter(15.seconds)
      .through(mail)
      .compile
      .drain
      .unsafeRunSync()
  }
}
