package mtest.guard

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.{SimpleEmailService, SimpleNotificationService, SimpleQueueService}
import com.github.chenharryhua.nanjin.common.aws.SqsConfig
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.*
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.guard.translators.{Attachment, SlackApp, Translator}
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies
import skunk.Session

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.ObserversTest"
class ObserversTest extends AnyFunSuite {

  def ok(agent: Agent[IO]) = agent.action("ok", _.notice).retry(IO(1)).run

  test("1.logging") {
    TaskGuard[IO]("logging")
      .service("text")
      .withRestartPolicy(constant_1hour)
      .updateConfig(_.withMetricReport(hourly).withQueueCapacity(20))
      .eventStream { ag =>
        val err = ag.action("error", _.critical).retry(err_fun(1)).run
        ok(ag) >> err.attempt
      }
      .evalTap(logging.verbose[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("2.console - text") {
    TaskGuard[IO]("console")
      .service("text")
      .withRestartPolicy(constant_1hour)
      .updateConfig(_.withMetricReport(secondly).withQueueCapacity(20))
      .eventStream { ag =>
        val err = ag.action("error", _.critical).retry(err_fun(1)).run
        ok(ag) >> err.attempt

      }
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("3.console - json") {
    TaskGuard[IO]("console")
      .service("json")
      .withRestartPolicy(constant_1hour)
      .updateConfig(_.withMetricReport(secondly).withQueueCapacity(20))
      .eventStream { ag =>
        val err = ag.action("error", _.critical).retry(err_fun(1)).run
        ok(ag) >> err.attempt
      }
      .evalTap(console(Translator.simpleJson[IO].map(_.noSpaces)))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("4.slack") {
    TaskGuard[IO]("sns")
      .updateConfig(_.withHomePage("https://abc.com/efg"))
      .service("slack")
      .withRestartPolicy(RetryPolicies.alwaysGiveUp)
      .updateConfig(_.withMetricReport(secondly).withQueueCapacity(20))
      .eventStream { ag =>
        val err = ag.action("error", _.critical).retry(err_fun(1)).run
        ok(ag) >> err.attempt
      }
      .through(
        SlackObserver(SimpleNotificationService.fake[IO])
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
      .withRestartPolicy(constant_1hour)
      .updateConfig(_.withMetricReport(1.second))
      .eventStream { ag =>
        val err =
          ag.action("error", _.critical).retry(err_fun(1)).run
        ok(ag) >> err.attempt
      }
      .take(9)
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
      .withRestartPolicy(constant_1hour)
      .updateConfig(_.withMetricReport(1.second))
      .eventStream { ag =>
        val err = ag.action("error", _.critical).retry(err_fun(1)).run
        ok(ag) >> err.attempt
      }
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
      .eventStream { ag =>
        val err =
          ag.action("error", _.critical).retry(IO.raiseError[Int](new Exception("oops"))).run
        ok(ag) >> err.attempt
      }
      .evalTap(console(len.map(_.show)))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("8.syntax") {
    EmailObserver(SimpleEmailService.fake[IO]).withInterval(1.minute).withChunkSize(10)
    EmailObserver[IO](SimpleNotificationService.fake[IO])
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
        .eventStream(_.action("sql", _.notice).retry(IO(0)).run)
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
      .withRestartPolicy(constant_1hour)
      .updateConfig(_.withMetricReport(1.second).withMetricDailyReset)
      .eventStream(
        _.action("sqs", _.critical).retry(IO.raiseError(new Exception)).run.delayBy(3.seconds).foreverM)
      .interruptAfter(7.seconds)
      .through(sqs.observe(SqsConfig.Fifo("https://google.com/abc.fifo")))
      .compile
      .drain
      .unsafeRunSync()
  }
}
