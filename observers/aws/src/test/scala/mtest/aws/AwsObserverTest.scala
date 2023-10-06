package mtest.aws

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.aws.{CloudWatch, SimpleEmailService, SimpleNotificationService, SimpleQueueService}
import com.github.chenharryhua.nanjin.common.aws.{SnsArn, SqsConfig}
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.{DurationUnit, NJEvent}
import com.github.chenharryhua.nanjin.guard.observers.{CloudWatchObserver, EmailObserver, SlackObserver, SqsObserver}
import eu.timepit.refined.auto.*
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class AwsObserverTest extends AnyFunSuite {
  val service: fs2.Stream[IO, NJEvent] =
    TaskGuard[IO]("nanjin")
      .service("observing")
      .withBrief(Json.fromString("brief"))
      .updateConfig(_.withRestartPolicy(policies.fixedRate(1.second)))
      .eventStream { ag =>
        val box = ag.atomicBox(1)
        val job = // fail twice, then success
          box.getAndUpdate(_ + 1).map(_ % 3 == 0).ifM(IO(1), IO.raiseError[Int](new Exception("oops")))
        val meter = ag.meter("meter", DurationUnit.SECONDS).counted
        val action = ag
          .action(
            "nj_error",
            _.critical.bipartite.timed.counted.policy(policies.fixedRate(1.second).limited(1)))
          .retry(job)
          .logInput(Json.fromString("input data"))
          .logOutput(_ => Json.fromString("output data"))
          .logErrorM(ex =>
            IO.delay(
              Json.obj("developer's advice" -> "no worries".asJson, "message" -> ex.getMessage.asJson)))
          .run

        val counter   = ag.counter("nj counter").asRisk
        val histogram = ag.histogram("nj histogram", DurationUnit.SECONDS).counted
        val alert     = ag.alert("nj alert")
        val gauge     = ag.gauge("nj gauge")

        gauge
          .register(100)
          .surround(
            gauge.timed.surround(
              action >>
                meter.mark(1000) >>
                counter.inc(10000) >>
                histogram.update(10000000000000L) >>
                alert.error("alarm") >>
                ag.metrics.report))
      }

  test("1.sqs") {
    val sqs = SqsObserver(SimpleQueueService.fake[IO](1.seconds, "")).updateTranslator(_.skipActionDone)
    service.through(sqs.observe(SqsConfig.Fifo("https://google.com/abc.fifo"))).compile.drain.unsafeRunSync()
  }

  test("2.ses mail") {
    val mail =
      EmailObserver(SimpleEmailService.fake[IO]).withInterval(5.seconds).withChunkSize(100).withOldestFirst

    service
      .through(mail.observe("abc@google.com", NonEmptyList.one("efg@tek.com"), "title"))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("3.syntax") {
    EmailObserver(SimpleEmailService.fake[IO]).withInterval(1.minute).withChunkSize(10).updateTranslator {
      _.skipMetricReset.skipMetricReport.skipActionStart.skipActionRetry.skipActionFail.skipActionDone.skipServiceAlert.skipServiceStart.skipServicePanic.skipServiceStop.skipAll
    }
  }

  test("4.slack") {
    val snsArn: SnsArn = SnsArn("arn:aws:sns:aaaa:123456789012:bb")
    service
      .through(SlackObserver(SimpleNotificationService.fake[IO]).at("@chenh").observe(snsArn))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("5.cloudwatch") {
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
    service.through(cloudwatch.observe("cloudwatch")).compile.drain.unsafeRunSync()
  }
}
