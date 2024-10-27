package mtest.aws

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.observers.cloudwatch.CloudWatchObserver
import com.github.chenharryhua.nanjin.guard.observers.ses.EmailObserver
import com.github.chenharryhua.nanjin.guard.observers.sns.SlackObserver
import com.github.chenharryhua.nanjin.guard.observers.sqs.SqsObserver
import eu.timepit.refined.auto.*
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class AwsObserverTest extends AnyFunSuite {
  val service: fs2.Stream[IO, NJEvent] =
    fs2.Stream.eval(AtomicCell[IO].of(1)).flatMap { box =>
      TaskGuard[IO]("nanjin")
        .updateConfig(_.withHomePage("http://abc.efg").addBrief(Json.fromString("brief")))
        .service("observing")
        .updateConfig(
          _.withRestartPolicy(Policy.fixedRate(1.second)).withMetricReport(Policy.crontab(_.secondly)))
        .eventStream { agent =>
          val ag = agent.facilitator("job").metrics
          val job =
            box.getAndUpdate(_ + 1).map(_ % 12 == 0).ifM(IO(1), IO.raiseError[Int](new Exception("oops")))
          val env = for {
            meter <- ag.meter("meter", _.withUnit(_.COUNT))
            action <- agent.action("nj_error").retry(job).buildWith(identity)
            counter <- ag.counter("nj counter", _.asRisk)
            histogram <- ag.histogram("nj histogram", _.withUnit(_.SECONDS))
            _ <- ag.gauge("nj gauge").register(box.get)
          } yield meter.update(1) >> action.run(()) >> counter.inc(1) >>
            histogram.update(1) >> agent.facilitator("abc").messenger.info(1) >> agent.adhoc.report
          env.use(identity)
        }
    }

  test("1.sqs") {
    val sqs = SqsObserver(sqs_client(1.seconds, ""))
    service.through(sqs.observe("https://google.com/abc.fifo", "group.id")).compile.drain.unsafeRunSync()
  }

  test("2.ses mail") {
    val mail =
      EmailObserver(ses_client)
        .withPolicy(Policy.fixedDelay(5.seconds), sydneyTime)
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
      _.skipMetricReset.skipMetricReport.skipServiceMessage.skipServiceStart.skipServicePanic.skipServiceStop.skipAll
    }
  }

  test("4.slack") {
    val snsArn: SnsArn = SnsArn("arn:aws:sns:aaaa:123456789012:bb")
    service.through(SlackObserver(sns_client).at("@chenh").observe(snsArn)).compile.drain.unsafeRunSync()
  }

  test("5.cloudwatch") {
    val cloudwatch = CloudWatchObserver(cloudwatch_client)
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
      .withTimeUnit(_.MICROSECONDS)
      .withInfoUnit(_.BITS)
      .withRateUnit(_.BYTES_SECOND)
    service.through(cloudwatch.observe("cloudwatch")).compile.drain.unsafeRunSync()
  }

  test("6. email observer - limited should terminate") {
    val mail =
      EmailObserver(ses_client)
        .withPolicy(Policy.fixedDelay(2.seconds).limited(3), sydneyTime)
        .observe("a@b.c", NonEmptyList.one("b@c.d"), "email")

    TaskGuard[IO]("email")
      .service("email")
      .updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))
      .eventStream(_ => IO.never)
      .through(mail)
      .compile
      .drain
      .unsafeRunSync()
  }
}
