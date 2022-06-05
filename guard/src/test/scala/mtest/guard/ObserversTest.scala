package mtest.guard

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.{SimpleEmailService, SimpleNotificationService, SimpleQueueService}
import com.github.chenharryhua.nanjin.common.aws.{SqsConfig, SqsUrl}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.*
import com.github.chenharryhua.nanjin.guard.translators.{Attachment, SlackApp, Translator}
import eu.timepit.refined.auto.*
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import skunk.Session

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.ObserversTest"
class ObserversTest extends AnyFunSuite {

  test("1.logging") {
    TaskGuard[IO]("logging")
      .service("text")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(hourly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("logging").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag
          .run(IO.raiseError(new Exception("oops")))
          .attempt
      }
      .evalTap(logging.verbose[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("2.console - text") {
    TaskGuard[IO]("console")
      .service("text")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(secondly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("console").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.retry(IO(1)).logInput("hello, world".asJson).logOutput(_.asJson).run >> ag
          .alert("notify")
          .error("error.msg") >> ag.run(IO.raiseError(new Exception("oops"))).attempt
      }
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("3.console - json") {
    TaskGuard[IO]("console")
      .service("json")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(secondly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("console").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag
          .run(IO.raiseError(new Exception("oops")))
          .attempt
      }
      .evalTap(console(Translator.verboseJson[IO].map(_.noSpaces)))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("4.slack") {
    TaskGuard[IO]("sns")
      .updateConfig(_.withHomePage("https://abc.com/efg"))
      .service("slack")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(secondly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("slack").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.retry(IO(1)).logInput("hello world".asJson).logOutput(_.asJson).run >> ag
          .alert("notify")
          .error("error.msg") >> ag.run(IO.raiseError(new Exception("oops")))
      }
      .interruptAfter(7.seconds)
      .through(SlackObserver(SimpleNotificationService.fake[IO])
        .withInterval(50.seconds)
        .at("@chenh")
        .observe(snsArn))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("5.ses mail") {
    val mail =
      EmailObserver(SimpleEmailService.fake[IO])
        .withInterval(5.seconds)
        .withChunkSize(100)
        .withOldestFirst
        .updateTranslator(_.skipActionStart)

    TaskGuard[IO]("ses")
      .updateConfig(_.withHomePage("https://google.com"))
      .service("ses")
      .updateConfig(_.withMetricReport(1.second).withConstantDelay(100.second))
      .eventStream(
        _.span("mail").max(0).critical.run(IO.raiseError(new Exception)).delayBy(3.seconds).foreverM)
      .take(2)
      .through(mail.observe("abc@google.com", NonEmptyList.one("efg@tek.com"), "title"))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("6.sns mail") {
    val mail =
      EmailObserver[IO](SimpleNotificationService.fake[IO])
        .withInterval(5.seconds)
        .withOldestFirst
        .withChunkSize(100)
        .updateTranslator(_.skipActionStart)

    TaskGuard[IO]("sns")
      .updateConfig(_.withHomePage("https://google.com"))
      .service("sns")
      .updateConfig(_.withMetricReport(1.second).withConstantDelay(100.second))
      .eventStream(
        _.span("mail").max(0).critical.run(IO.raiseError(new Exception)).delayBy(3.seconds).foreverM)
      .interruptAfter(7.seconds)
      .through(mail.observe(snsArn, "title"))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("7.lense") {
    val len =
      Translator
        .serviceStart[IO, SlackApp]
        .modify(_.map(s =>
          s.copy(attachments = Attachment("modified by lense", List.empty) :: s.attachments)))
        .apply(Translator.slack[IO])

    TaskGuard[IO]("lense")
      .service("lense")
      .eventStream(_.span("lense").notice.run(IO(())))
      .interruptAfter(3.seconds)
      .evalTap(console(len.map(_.show)))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("8.syntax") {
    EmailObserver(SimpleEmailService.fake[IO]).withInterval(1.minute).withChunkSize(10)
    val eo = EmailObserver[IO](SimpleNotificationService.fake[IO])
      .withInterval(1.minute)
      .withChunkSize(10)
      .updateTranslator(
        _.skipMetricReset.skipMetricReport.skipActionStart.skipActionRetry.skipActionFail.skipActionSucc.skipInstantAlert.skipPassThrough.skipServiceStart.skipServicePanic.skipServiceStop.skipAll)

    logging.simple[IO]
    console.verbose[IO]
  }

  test("9.postgres") {
    import natchez.Trace.Implicits.noop
    import skunk.implicits.toStringOps

    val session: Resource[IO, Session[IO]] =
      Session.single[IO](
        host = "localhost",
        port = 5432,
        user = "postgres",
        database = "postgres",
        password = Some("postgres"),
        debug = true)

    val cmd =
      sql"""CREATE TABLE IF NOT EXISTS log (
              info json NULL,
              id SERIAL,
              timestamp timestamptz default current_timestamp)""".command

    val run = session.use(_.execute(cmd)) >>
      TaskGuard[IO]("postgres")
        .service("postgres")
        .eventStream(_.notice.run(IO(0)))
        .evalTap(console.verbose[IO])
        .through(PostgresObserver(session).observe("log"))
        .compile
        .drain

    run.unsafeRunSync()
  }
  test("10.sqs") {
    val sqs = SqsObserver(SimpleQueueService.fake[IO](1.seconds, "")).updateTranslator(_.skipActionSucc)
    TaskGuard[IO]("sqs")
      .service("sqs")
      .updateConfig(_.withMetricReport(1.second).withConstantDelay(100.second).withMetricDailyReset)
      .eventStream(
        _.span("sqs").max(0).critical.run(IO.raiseError(new Exception)).delayBy(3.seconds).foreverM)
      .interruptAfter(7.seconds)
      .through(sqs.observe(SqsConfig.Fifo("https://google.com/abc.fifo")))
      .compile
      .drain
      .unsafeRunSync()
  }
}
