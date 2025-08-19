package example

import cats.effect.IO
import com.comcast.ip4s.IpLiteralSyntax
import com.github.chenharryhua.nanjin.aws.{ec2, ecs}
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import fs2.Stream
import io.circe.Json
import io.circe.syntax.EncoderOps

import scala.concurrent.duration.DurationInt

object aws_task_template {
  val task: TaskGuard[IO] = TaskGuard[IO]("nanjin").updateConfig(
    _.withZoneId(sydneyTime)
      .withHomePage("https://github.com/chenharryhua/nanjin")
      .withHostName(ec2.private_ip / HostName.local_host)
      .withMetricReport(_.crontab(_.every15Minutes))
      .withMetricReset(_.crontab(_.daily.midnight))
      .withRestartPolicy(
        Policy
          .fixedDelay(3.seconds, 2.minutes, 1.hour)
          .limited(3)
          .followedBy(_.fixedRate(2.hours).limited(12))
          .followedBy(_.crontab(_.daily.tenAM)),
        5.hours)
      .addBrief(ecs.container_metadata[IO]))

  private val service1: Stream[IO, Event] = task
    .service("s1")
    .updateConfig(_.addBrief(Json.obj("a" -> 1.asJson)))
    .updateConfig(_.withHttpServer(_.withPort(port"1026")))
    .eventStream(_ => IO.never)

  private val service2: Stream[IO, Event] = task
    .service("s2")
    .updateConfig(_.addBrief(Json.obj("b" -> 2.asJson)))
    .updateConfig(_.withHttpServer(_.withPort(port"1027")))
    .eventStream(_ => IO.never)

  service1.merge(service2)

}
