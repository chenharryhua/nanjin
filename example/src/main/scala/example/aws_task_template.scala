package example

import cats.effect.IO
import com.comcast.ip4s.*
import com.github.chenharryhua.nanjin.aws.ecs
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import fs2.Stream
import io.circe.Json
import io.circe.syntax.EncoderOps

import scala.concurrent.duration.DurationInt

/** Example: a shared `TaskGuard` template configured for an AWS/ECS deployment, reused by the other examples
  * (e.g. `kafka_connector_s3`).
  *
  * `task` sets the timezone, homepage, a 15-minute metric report cadence, a multi-stage restart policy, an
  * ECS container-metadata brief, and event-history capacity. `merged` shows running two independent services
  * (each on its own HTTP port, with its own brief) as a single merged event stream.
  */
object aws_task_template {
  val task: TaskGuard[IO] = TaskGuard[IO]("nanjin").updateConfig(
    _.withZoneId(sydneyTime)
      .withHomepage("https://github.com/chenharryhua/nanjin")
      .withReportPolicy(_.crontab(_.every15Minutes))
      // on failure: retry with a fixed delay (up to 3 times), then a fixed rate (up to 12 times), then daily
      // at 10am; if the service stays healthy for 8+ hours, the policy resets to the initial stage
      .withRestartPolicy(
        8.hours,
        _.fixedDelay(3.seconds, 2.minutes, 1.hour)
          .limited(3)
          .followedBy(_.fixedRate(2.hours).limited(12))
          .followedBy(_.crontab(_.daily.tenAM)))
      .addBrief(ecs.containerMetadata[IO]) // attach ECS container metadata to every event
      .withHistoryCapacity(32, 32, 32)
  )

  // two independent services derived from the same task, each with its own brief and HTTP port
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

  /** The two services running concurrently as one event stream. */
  val merged: Stream[IO, Event] = service1.merge(service2)

}
