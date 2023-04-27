package mtest.guard

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.{
  CloudWatch,
  SimpleEmailService,
  SimpleNotificationService,
  SimpleQueueService
}
import com.github.chenharryhua.nanjin.common.aws.SqsConfig
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.*
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.guard.translators.{Attachment, SlackApp, Translator}
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.{InfluxDBClientFactory, InfluxDBClientOptions}
import eu.timepit.refined.auto.*
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies
import skunk.Session
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.ObserversTest"
class ObserversTest extends AnyFunSuite {

  def ok(agent: Agent[IO]) = agent.action("nj_ok", _.notice).retry(IO(1)).run

  val service =
    TaskGuard[IO]("nanjin")
      .service("observing")
      .withBrief(Json.fromString("brief"))
      .withRestartPolicy(constant_1hour)
      .updateConfig(_.withMetricReport(cron_1second))
      .eventStream { ag =>
        val ok = ag
          .action("nj_ok", _.trivial.notice.withTiming.withCounting)
          .retry(IO(1))
          .logInput(Json.fromString("ok input"))
          .logOutput(_ => Json.fromString("ok output"))
          .run
        val meter = ag.meter("meter", StandardUnit.SECONDS).withCounting
        val err = ag
          .action("nj_error", _.critical.notice.withTiming.withCounting)
          .withRetryPolicy(RetryPolicies.constantDelay[IO](1.second).join(RetryPolicies.limitRetries(1)))
          .retry(IO.raiseError[Int](new Exception("oops")))
          .logInput(Json.fromString("error input data"))
          .logOutput(_ => Json.fromString("error output data"))
          .logErrorM(ex => IO(Json.fromString(ex.getMessage)))
          .run
          .attempt
        val counter   = ag.counter("nj counter").asRisk
        val histogram = ag.histogram("nj histogram", StandardUnit.SECONDS).withCounting
        val alert     = ag.alert("nj alert")
        val gauge     = ag.gauge("nj gauge")

        gauge
          .register(1000000)
          .surround(
            gauge.timed.surround(
              ok >>
                meter.mark(1000000000) >>
                counter.inc(10000000) >>
                histogram.update(1000000000) >>
                alert.error("alarm") >>
                err >>
                ag.metrics.report))
      }
      .take(16)

  test("1.logging verbose") {
    service.evalTap(logging.verbose[IO]).compile.drain.unsafeRunSync()
  }

  test("1.2.logging json") {
    service.evalTap(logging.json[IO].withLoggerName("json")).compile.drain.unsafeRunSync()
  }
  test("1.3 logging simple") {
    service.evalTap(logging.simple[IO]).compile.drain.unsafeRunSync()
  }

  test("2.console - simple text") {
    service.evalTap(console.simple[IO]).compile.drain.unsafeRunSync()
  }

  test("3.console - pretty json") {
    service.evalTap(console.json[IO]).compile.drain.unsafeRunSync()
  }

  test("3.1.console - simple json") {
    service.evalTap(console.simpleJson[IO]).compile.drain.unsafeRunSync()
  }

  test("3.2.console - verbose json") {
    service.evalTap(console.verboseJson[IO]).compile.drain.unsafeRunSync()
  }

  test("4.slack") {
    service
      .through(SlackObserver(SimpleNotificationService.fake[IO]).at("@chenh").observe(snsArn))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("5.ses mail") {
    val mail =
      EmailObserver(SimpleEmailService.fake[IO]).withInterval(5.seconds).withChunkSize(100).withOldestFirst

    service
      .through(mail.observe("abc@google.com", NonEmptyList.one("efg@tek.com"), "title"))
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
    EmailObserver(SimpleEmailService.fake[IO]).withInterval(1.minute).withChunkSize(10).updateTranslator {
      _.skipMetricReset.skipMetricReport.skipActionStart.skipActionRetry.skipActionFail.skipActionComplete.skipServiceAlert.skipServiceStart.skipServicePanic.skipServiceStop.skipAll
    }
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
      service.evalTap(console.verbose[IO]).through(PostgresObserver(session).observe("log")).compile.drain

    run.unsafeRunSync()
  }
  test("10.sqs") {
    val sqs = SqsObserver(SimpleQueueService.fake[IO](1.seconds, "")).updateTranslator(_.skipActionComplete)
    TaskGuard[IO]("sqs")
      .service("sqs")
      .withRestartPolicy(constant_1hour)
      .updateConfig(_.withMetricReport(cron_1second).withMetricDailyReset)
      .eventStream(
        _.action("sqs", _.critical).retry(IO.raiseError(new Exception)).run.delayBy(3.seconds).foreverM)
      .interruptAfter(7.seconds)
      .through(sqs.observe(SqsConfig.Fifo("https://google.com/abc.fifo")))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("11.influx db") {
    val options = InfluxDBClientOptions
      .builder()
      .url("http://localhost:8086")
      .authenticate("chenh", "chenhchenh".toCharArray)
      .bucket("nanjin")
      .org("nanjin")
      .build()

    val influx = InfluxdbObserver[IO](IO(InfluxDBClientFactory.create(options)))
      .withWriteOptions(_.batchSize(1))
      .withWritePrecision(WritePrecision.NS)
      .addTag("tag", "customer")
      .addTags(Map("a" -> "b"))
    service.evalTap(console.simple[IO]).through(influx.observe).compile.drain.unsafeRunSync()
  }
  test("12.cloudwatch") {
    val cloudwatch = CloudWatchObserver(CloudWatch.fake[IO])
      .withStorageResolution(10)
      .withMax
      .withMin
      .withMean
      .withStdDev
      .withP50
      .withP75
      .withP95
      .withP98
      .withP99
      .withP999
    TaskGuard[IO]("cloudwatch")
      .service("cloudwatch")
      .withRestartPolicy(constant_1hour)
      .updateConfig(_.withMetricReport(cron_1second).withMetricDailyReset)
      .eventStream(
        _.action("cloudwatch", _.critical.withTiming.withCounting)
          .retry(IO(()))
          .run
          .delayBy(2.seconds)
          .foreverM)
      .interruptAfter(5.seconds)
      .through(cloudwatch.observe("cloudwatch"))
      .compile
      .drain
      .unsafeRunSync()
  }
}
