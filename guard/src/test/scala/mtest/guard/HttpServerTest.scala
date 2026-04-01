package mtest.guard

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.comcast.ip4s.port
import com.github.chenharryhua.nanjin.common.chrono.zones.londonTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event.{ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.StopReason.Maintenance
import io.circe.{jawn, Json}
import org.http4s.Method.POST
import org.http4s.Request
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.implicits.uri
import org.scalatest.funsuite.AnyFunSuite
import squants.information.{Bytes, Megabytes}

import scala.concurrent.duration.*

class HttpServerTest extends AnyFunSuite {
  val guard: TaskGuard[IO] = TaskGuard[IO]("http").updateConfig(
    _.withHomePage("https://abc.com/efg")
      .withZoneId(londonTime)
      .withRestartPolicy(1.hour, _.fixedDelay(1.seconds))
      .withRealTimeMetrics(100, _.crontab(_.every5Minutes))
  )

  test("1.stop service") {
    val stop = Request[IO](method = POST, uri = uri"http://localhost:9999/service/stop")
    val client = EmberClientBuilder
      .default[IO]
      .build
      .use { c =>
        c.expect[String]("http://localhost:9999/metrics/report") >>
          c.expect[String]("http://localhost:9999/metrics/reset") >>
          c.expect[String]("http://localhost:9999/metrics/history") >>
          c.expect[String]("http://localhost:9999/service/jvm") >>
          c.expect[String]("http://localhost:9999/service/params") >>
          c.expect[String]("http://localhost:9999/service/health_check") >>
          c.expect[String]("http://localhost:9999/panics") >>
          c.expect[String]("http://localhost:9999/errors") >>
          c.expect[String]("http://localhost:9999/alarm/level") >>
          c.expect[String](stop)
      }
      .delayBy(5.seconds)

    val run =
      guard
        .service("http stop")
        .updateConfig(
          _.withMetricReport(_.crontab(_.secondly))
            .withHttpServer(_.withPort(port"9999"))
            .withLogFormat(_.Slf4j_Json_OneLine))
        .eventStream { agent =>
          agent
            .facilitate("test") { ag =>
              for {
                _ <- ag.gauge("a", _.register(IO(1)))
                _ <- ag.counter("a").evalMap(_.inc(1))
                _ <- ag.histogram("a", _.enable(true).withUnit(Bytes)).evalMap(_.update(1))
                _ <- ag.meter("a", _.withUnit(Megabytes)).evalMap(_.mark(1))
              } yield Kleisli((_: Int) => IO.unit)
            }
            .use(_.run(1) >> agent.adhoc.report >> IO.sleep(10.hours))
        }
        .map(checkJson)
        .compile
        .toList <& client
    val res = run.unsafeRunSync()
    assert(res.head.isInstanceOf[ServiceStart])
    assert(res.last.asInstanceOf[ServiceStop].cause === Maintenance)
  }

  test("3.panic history") {
    val stop = Request[IO](method = POST, uri = uri"http://localhost:9997/service/stop")
    val client = EmberClientBuilder
      .default[IO]
      .build
      .use { c =>
        c.expect[String]("http://localhost:9997/panics")
          .map(j =>
            assert(
              jawn.parse(j).toOption.get.hcursor.downField("history")
                .as[List[Json]].toOption.get.size > 2)) >>
          c.expect[String](stop)
      }
      .delayBy(5.seconds)

    val res = TaskGuard[IO]("panic")
      .service("history")
      .updateConfig(
        _.withRestartPolicy(1.hour, _.fixedDelay(1.second)).withHttpServer(_.withPort(port"9997")))
      .eventStream(_ => IO.raiseError(new Exception))
      .map(checkJson)
      .compile
      .drain &> client
    res.unsafeRunSync()
  }
}
