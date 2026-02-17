package mtest.aws

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.observers.cloudwatch.CloudWatchObserver
import com.github.chenharryhua.nanjin.guard.observers.ses.EmailObserver
import com.github.chenharryhua.nanjin.guard.observers.sqs.SqsObserver
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite
import squants.information.Bytes
import squants.information.InformationConversions.InformationConversions
import squants.mass.MassConversions.MassConversions
import squants.mass.Micrograms

import scala.concurrent.duration.DurationInt

class AwsObserverTest extends AnyFunSuite {
  private val service: fs2.Stream[IO, Event] = TaskGuard[IO]("aws")
    .service("test")
    .updateConfig(_.addBrief("brief").withRestartPolicy(10.hours, _.fixedDelay(1.second).limited(1)))
    .eventStream { agent =>
      agent
        .facilitate("metrics")(_.meter(Bytes)("meter"))
        .use(
          _.run(10.bytes) >>
            agent.herald.done("good") >>
            agent.herald.error(new Exception("oops oops oops oops oops oops oops oops"))("my error") >>
            agent.adhoc.report) >> IO.raiseError(new Exception)
    }

  test("1.sqs") {
    //  val sqs =
    SqsObserver(sqs_client(1.seconds, ""))
    //  service.through(sqs.observe("https://google.com/abc.fifo", "group.id")).compile.drain.unsafeRunSync()
  }

  test("2.ses mail") {
    val mail =
      EmailObserver(ses_client)
        .withPolicy(_.fixedDelay(5.seconds))
        .withZoneId(sydneyTime)
        .withCapacity(200)
        .withOldestFirst

    service
      .through(mail.observe("abc@google.com", NonEmptyList.one("efg@tek.com"), "title"))
      .debug()
      .compile
      .drain
      .unsafeRunSync()
  }

  test("3.syntax") {
    EmailObserver(ses_client).updateTranslator {
      _.skipMetricsReset.skipMetricsReport.skipServiceMessage.skipServiceStart.skipServicePanic.skipServiceStop.skipAll
    }
  }

  test("4.slack") {
    //   val snsArn: SnsArn = SnsArn("arn:aws:sns:aaaa:123456789012:bb")
    //   service.through(SlackObserver(sns_client).at("@chenh").observe(snsArn)).compile.drain.unsafeRunSync()
  }

  test("5.cloudwatch") {
    val service = TaskGuard[IO]("aws")
      .service("cloudwatch")
      .eventStream { agent =>
        agent.facilitate("metrics")(_.meter(Micrograms)("meter-x")).use { m =>
          m.run(1.mg) >> agent.adhoc.report >> IO.sleep(1.second) >>
            m.run(2.mg) >> agent.adhoc.report >> IO.sleep(1.second) >>
            m.run(2.mg) >> agent.adhoc.report >> IO.sleep(1.second) >>
            m.mark(10) >> agent.adhoc.report
        }
      }
      .debug()

    val cloudwatch = CloudWatchObserver(cloudwatch_client).withHighStorageResolution

    service.through(cloudwatch.observe("cloudwatch")).compile.drain.unsafeRunSync()
  }

  test("6. email observer - limited should terminate") {
    val mail =
      EmailObserver(ses_client)
        .withPolicy(_.fixedDelay(2.seconds).limited(3))
        .withZoneId(sydneyTime)
        .observe("a@b.c", NonEmptyList.one("b@c.d"), "email")

    TaskGuard[IO]("email")
      .service("email")
      .updateConfig(_.withMetricReport(_.crontab(_.secondly)))
      .eventStream(_ => IO.never)
      .through(mail)
      .compile
      .drain
      .unsafeRunSync()
  }
}
