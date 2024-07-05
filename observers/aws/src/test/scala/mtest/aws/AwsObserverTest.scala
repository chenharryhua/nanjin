package mtest.aws

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.aws.{SnsArn, SqsConfig}
import com.github.chenharryhua.nanjin.common.chrono.policies
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
          _.withRestartPolicy(policies.fixedRate(1.second)).withMetricReport(policies.crontab(_.secondly)))
        .eventStream { ag =>
          val job =
            box.getAndUpdate(_ + 1).map(_ % 12 == 0).ifM(IO(1), IO.raiseError[Int](new Exception("oops")))
          val env = for {
            meter <- ag.meter("meter", _.withUnit(_.COUNT).counted)
            action <- ag
              .action(
                "nj_error",
                _.critical.bipartite.timed.counted.policy(policies.fixedRate(1.second).limited(3)))
              .retry(job)
              .buildWith(identity)
            counter <- ag.counter("nj counter", _.asRisk)
            histogram <- ag.histogram("nj histogram", _.withUnit(_.SECONDS).counted)
            alert <- ag.alert("nj alert")
            _ <- ag.gauge("nj gauge").register(box.get)
          } yield meter.update(1) >> action.run(()) >> counter.inc(1) >>
            histogram.update(1) >> alert.info(1) >> ag.metrics.report
          env.use(identity)
        }
    }

  test("1.sqs") {
    val sqs = SqsObserver(sqs_client(1.seconds, "")).updateTranslator(_.skipActionDone)
    service.through(sqs.observe(SqsConfig.Fifo("https://google.com/abc.fifo"))).compile.drain.unsafeRunSync()
  }

  test("2.ses mail") {
    val mail =
      EmailObserver(ses_client).withInterval(5.seconds).withChunkSize(100).withOldestFirst

    service
      .through(mail.observe("abc@google.com", NonEmptyList.one("efg@tek.com"), "title"))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("3.syntax") {
    EmailObserver(ses_client).withInterval(1.minute).withChunkSize(10).updateTranslator {
      _.skipMetricReset.skipMetricReport.skipActionStart.skipActionRetry.skipActionFail.skipActionDone.skipServiceAlert.skipServiceStart.skipServicePanic.skipServiceStop.skipAll
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
}
