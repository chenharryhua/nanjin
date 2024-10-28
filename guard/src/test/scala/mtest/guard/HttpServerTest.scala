package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.comcast.ip4s.IpLiteralSyntax
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.chrono.zones.londonTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import io.circe.{jawn, Json}
import org.http4s.ember.client.EmberClientBuilder
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class HttpServerTest extends AnyFunSuite {
  val guard: TaskGuard[IO] = TaskGuard[IO]("http").updateConfig(
    _.withHomePage("https://abc.com/efg")
      .withZoneId(londonTime)
      .withRestartPolicy(Policy.fixedDelay(1.seconds)))

  test("1.stop service") {
    val client = EmberClientBuilder
      .default[IO]
      .build
      .use { c =>
        c.expect[String]("http://localhost:9999/index.html") >>
          c.expect[String]("http://localhost:9999/metrics/yaml") >>
          c.expect[String]("http://localhost:9999/metrics/vanilla") >>
          c.expect[String]("http://localhost:9999/metrics/json") >>
          c.expect[String]("http://localhost:9999/metrics/reset") >>
          c.expect[String]("http://localhost:9999/metrics/jvm") >>
          c.expect[String]("http://localhost:9999/metrics/history") >>
          c.expect[String]("http://localhost:9999/service/params") >>
          c.expect[String]("http://localhost:9999/service/health_check") >>
          c.expect[String]("http://localhost:9999/service/history") >>
          c.expect[String]("http://localhost:9999/service/stop")
      }
      .delayBy(5.seconds)

    val res =
      guard
        .service("http stop")
        .updateConfig(_.withMetricReport(Policy.crontab(_.secondly)).withHttpServer(_.withPort(port"9999")))
        .eventStream { agent =>
          val ag = agent.facilitator("test").metrics
          val m = for {
            _ <- ag.gauge("a").register(IO(1))
            _ <- ag.counter("a").evalMap(_.inc(1))
            _ <- ag.histogram("a", _.withUnit(_.BYTES)).evalMap(_.update(1))
            _ <- ag.meter("a", _.withUnit(_.MEGABYTES)).evalMap(_.update(1))
          } yield ()

          m.surround(agent.adhoc.report >> IO.sleep(10.hours))
        }
        .map(checkJson)
        .compile
        .toList <& client
    res.unsafeRunSync()
  }

  test("2.service panic") {
    val client = EmberClientBuilder
      .default[IO]
      .build
      .use { c =>
        c.expect[String]("http://localhost:9998/service/health_check").attempt.map(r => assert(r.isLeft)) >>
          c.expect[String]("http://localhost:9998/service/stop")
      }
      .delayBy(2.seconds)
    val res = TaskGuard[IO]("panic")
      .service("panic")
      .updateConfig(_.withRestartPolicy(Policy.fixedDelay(1.hour)).withHttpServer(_.withPort(port"9998")))
      .eventStream {
        _.facilitator("panic").action(IO.raiseError[Int](new Exception)).buildWith(identity).use(_.run(()))
      }
      .map(checkJson)
      .compile
      .drain &> client
    res.unsafeRunSync()
  }

  test("3.panic history") {
    val client = EmberClientBuilder
      .default[IO]
      .build
      .use { c =>
        c.expect[String]("http://localhost:9997/service/history")
          .map(j =>
            assert(
              jawn
                .parse(j)
                .toOption
                .get
                .hcursor
                .downField("history")
                .as[List[Json]]
                .toOption
                .get
                .size > 2)) >>
          c.expect[String]("http://localhost:9997/service/stop")
      }
      .delayBy(5.seconds)

    val res = TaskGuard[IO]("panic")
      .service("history")
      .updateConfig(_.withRestartPolicy(Policy.fixedDelay(1.second)).withHttpServer(_.withPort(port"9997")))
      .eventStream {
        _.facilitator("panic history")
          .action(IO.raiseError[Int](new Exception))
          .buildWith(identity)
          .use(_.run(()))
      }
      .map(checkJson)
      .compile
      .drain &> client
    res.unsafeRunSync()
  }

  test("4.monitor") {
    val client = EmberClientBuilder
      .default[IO]
      .build
      .use { c =>
        c.expect[String]("http://localhost:9996/metrics/json") >>
          c.expect[String]("http://localhost:9996/service/stop")
      }
      .delayBy(5.seconds)
    val res = TaskGuard[IO]("never")
      .service("never")
      .updateConfig(_.withHttpServer(_.withPort(port"9996")))
      .eventStream {
        _.facilitator("panic").action(IO.sleep(1.seconds)).buildWith(identity).use(_.run(())).foreverM
      }
      .map(checkJson)
      .compile
      .drain &> client
    res.unsafeRunSync()
  }
}
