package mtest.guard

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.{SimpleEmailService, SimpleNotificationService}
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.crontabs
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.*
import com.github.chenharryhua.nanjin.guard.translators.{Attachment, SlackApp, Translator}
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite
import skunk.Session

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.ObserversTest"
class ObserversTest extends AnyFunSuite {
  val snsArn: SnsArn = SnsArn("arn:aws:sns:aaaa:123456789012:bb")

  test("logging") {
    TaskGuard[IO]("logging")
      .service("text")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(crontabs.hourly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("logging").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag.run(IO.raiseError(new Exception("oops"))).attempt
      }
      .evalTap(logging.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("console - text") {
    TaskGuard[IO]("console")
      .service("text")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(crontabs.secondly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("console").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag.run(IO.raiseError(new Exception("oops"))).attempt
      }
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("console - json") {
    TaskGuard[IO]("console")
      .service("json")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(crontabs.secondly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("console").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag.run(IO.raiseError(new Exception("oops"))).attempt
      }
      .evalTap(console(Translator.simpleJson[IO].map(_.noSpaces)))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("slack") {
    TaskGuard[IO]("sns")
      .updateConfig(_.withHomePage("https://abc.com/efg"))
      .service("slack")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(crontabs.secondly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("slack").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag.run(IO.raiseError(new Exception("oops")))
      }
      .interruptAfter(7.seconds)
      .through(SlackPipe[IO](snsArn, SimpleNotificationService.fake[IO]).at("@chenh"))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("ses mail") {
    val mail =
      SesEmailObserver[IO]("abc@google.com", NonEmptyList.one("efg@tek.com"), SimpleEmailService.fake[IO])
        .withInterval(5.seconds)
        .withChunkSize(100)
        .withSubject("subject")
        .updateTranslator(_.skipActionStart)

    TaskGuard[IO]("ses")
      .updateConfig(_.withHomePage("https://google.com"))
      .service("ses")
      .updateConfig(_.withMetricReport(1.second).withConstantDelay(100.second))
      .eventStream(_.span("mail").max(0).critical.run(IO.raiseError(new Exception)).delayBy(3.seconds).foreverM)
      .interruptAfter(7.seconds)
      .through(mail)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("sns mail") {
    val mail =
      SnsEmailObserver[IO](snsArn, SimpleNotificationService.fake[IO])
        .withInterval(5.seconds)
        .withChunkSize(100)
        .updateTranslator(_.skipActionStart)

    TaskGuard[IO]("sns")
      .updateConfig(_.withHomePage("https://google.com"))
      .service("sns")
      .updateConfig(_.withMetricReport(1.second).withConstantDelay(100.second))
      .eventStream(_.span("mail").max(0).critical.run(IO.raiseError(new Exception)).delayBy(3.seconds).foreverM)
      .interruptAfter(7.seconds)
      .through(mail)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("lense") {
    val len =
      Translator
        .serviceStart[IO, SlackApp]
        .modify(_.map(s => s.copy(attachments = Attachment("modified by lense", List.empty) :: s.attachments)))
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

  test("syntax") {
    SesEmailObserver[IO]("abc@google.com", NonEmptyList.one("efg@tek.com"), SimpleEmailService.fake[IO])
      .withSubject("subject")
      .withInterval(1.minute)
      .withChunkSize(10)
    SnsEmailObserver[IO](snsArn, SimpleNotificationService.fake[IO])
      .withTitle("title")
      .withInterval(1.minute)
      .withChunkSize(10)
    logging.simple[IO]
    console.verbose[IO]
  }

  test("postgres") {
    import skunk.implicits.toStringOps
    import natchez.Trace.Implicits.noop

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
        .through(PostgresPipe(session).withTableName("log"))
        .compile
        .drain

    run.unsafeRunSync()
  }
}
