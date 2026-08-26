package mtest.guard

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.comcast.ip4s.port
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
    _.withHomepage("https://abc.com/efg")
      .withZoneId(_.londonTime)
      .withRestartPolicy(1.hour, _.fixedDelay(1.seconds).repeat)
      .withDashboard(100, _.crontab(_.secondly).repeat)
      .withHistoryCapacity(32, 32, 32)
      .withLogThreshold(_.Info, _.Info))

  test("1.stop service") {
    val stop = Request[IO](method = POST, uri = uri"http://localhost:9999/stop")
    val client = EmberClientBuilder
      .default[IO]
      .build
      .use { c =>
        c.expect[String]("http://localhost:9999/metrics/report") >>
          c.expect[String]("http://localhost:9999/metrics/history") >>
          c.expect[String]("http://localhost:9999/metrics/jvm") >>
          c.expect[String]("http://localhost:9999/params") >>
          c.expect[String]("http://localhost:9999/health") >>
          c.expect[String]("http://localhost:9999/panics") >>
          c.expect[String]("http://localhost:9999/errors") >>
          c.expect[String]("http://localhost:9999/log/level") >>
          c.expect[String]("http://localhost:9999/healthcheck/status") >>
          c.expect[String](stop)
      }
      .delayBy(5.seconds)

    val run =
      guard
        .service("http stop")
        .updateConfig(_.withMetricsReport(_.crontab(_.secondly).repeat)
          .withHttpServer(_.withPort(port"9999")))
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

  test("2.log threshold - set both via POST /log/{level}") {
    val client = EmberClientBuilder
      .default[IO]
      .build
      .use { c =>
        val setWarn = Request[IO](method = POST, uri = uri"http://localhost:9998/log/Warn")
        val getLevel = uri"http://localhost:9998/log/level"
        for {
          before <- c.expect[String](getLevel)
          _ = assert(
            jawn.parse(before).toOption.get.hcursor.downField("logger").as[String].toOption.contains("Info"))
          resp <- c.expect[String](setWarn)
          parsed = jawn.parse(resp).toOption.get
          _ = assert(
            parsed.hcursor.downField("current").downField("logger").as[String].toOption.contains("Warn"))
          _ = assert(
            parsed.hcursor.downField("current").downField("channel").as[String].toOption.contains("Warn"))
          after <- c.expect[String](getLevel)
          _ = assert(
            jawn.parse(after).toOption.get.hcursor.downField("logger").as[String].toOption.contains("Warn"))
          stop <- c.expect[String](Request[IO](method = POST, uri = uri"http://localhost:9998/stop"))
        } yield stop
      }
      .delayBy(3.seconds)

    val res = guard
      .service("log-both")
      .updateConfig(_.withHttpServer(_.withPort(port"9998")))
      .eventStream(_ => IO.sleep(10.hours))
      .map(checkJson)
      .compile
      .drain <& client
    res.unsafeRunSync()
  }

  test("3.log threshold - POST /log/logger/{level} changes only logger") {
    val client = EmberClientBuilder
      .default[IO]
      .build
      .use { c =>
        val setLoggerDebug = Request[IO](method = POST, uri = uri"http://localhost:9996/log/logger/Debug")
        val getLevel = uri"http://localhost:9996/log/level"
        for {
          resp <- c.expect[String](setLoggerDebug)
          parsed = jawn.parse(resp).toOption.get
          _ = assert(
            parsed.hcursor.downField("current").downField("logger").as[String].toOption.contains("Debug"))
          _ = assert(
            parsed.hcursor.downField("current").downField("channel").as[String].toOption.contains("Info"))
          after <- c.expect[String](getLevel)
          _ = assert(
            jawn.parse(after).toOption.get.hcursor.downField("logger").as[String].toOption.contains("Debug"))
          _ = assert(
            jawn.parse(after).toOption.get.hcursor.downField("channel").as[String].toOption.contains("Info"))
          stop <- c.expect[String](Request[IO](method = POST, uri = uri"http://localhost:9996/stop"))
        } yield stop
      }
      .delayBy(3.seconds)

    val res = guard
      .service("log-logger")
      .updateConfig(_.withHttpServer(_.withPort(port"9996")))
      .eventStream(_ => IO.sleep(10.hours))
      .map(checkJson)
      .compile
      .drain <& client
    res.unsafeRunSync()
  }

  test("4.log threshold - POST /log/channel/{level} changes only channel") {
    val client = EmberClientBuilder
      .default[IO]
      .build
      .use { c =>
        val setChannelError = Request[IO](method = POST, uri = uri"http://localhost:9995/log/channel/Error")
        val getLevel = uri"http://localhost:9995/log/level"
        for {
          resp <- c.expect[String](setChannelError)
          parsed = jawn.parse(resp).toOption.get
          _ = assert(
            parsed.hcursor.downField("current").downField("channel").as[String].toOption.contains("Error"))
          _ = assert(
            parsed.hcursor.downField("current").downField("logger").as[String].toOption.contains("Info"))
          after <- c.expect[String](getLevel)
          _ = assert(
            jawn.parse(after).toOption.get.hcursor.downField("channel").as[String].toOption.contains("Error"))
          _ = assert(
            jawn.parse(after).toOption.get.hcursor.downField("logger").as[String].toOption.contains("Info"))
          stop <- c.expect[String](Request[IO](method = POST, uri = uri"http://localhost:9995/stop"))
        } yield stop
      }
      .delayBy(3.seconds)

    val res = guard
      .service("log-channel")
      .updateConfig(_.withHttpServer(_.withPort(port"9995")))
      .eventStream(_ => IO.sleep(10.hours))
      .map(checkJson)
      .compile
      .drain <& client
    res.unsafeRunSync()
  }

  test("5.log threshold - POST /log/logger/Disabled disables logging") {
    val client = EmberClientBuilder
      .default[IO]
      .build
      .use { c =>
        val disable = Request[IO](method = POST, uri = uri"http://localhost:9994/log/logger/Disabled")
        val getLevel = uri"http://localhost:9994/log/level"
        for {
          resp <- c.expect[String](disable)
          parsed = jawn.parse(resp).toOption.get
          _ = assert(parsed.hcursor.downField("current").as[String].toOption.contains("Disabled"))
          after <- c.expect[String](getLevel)
          _ = assert(jawn.parse(after).toOption.get.as[String].toOption.contains("Disabled"))
          stop <- c.expect[String](Request[IO](method = POST, uri = uri"http://localhost:9994/stop"))
        } yield stop
      }
      .delayBy(3.seconds)

    val res = guard
      .service("log-disable")
      .updateConfig(_.withHttpServer(_.withPort(port"9994")))
      .eventStream(_ => IO.sleep(10.hours))
      .map(checkJson)
      .compile
      .drain <& client
    res.unsafeRunSync()
  }

  test("6.log threshold - invalid level returns BadRequest") {
    val client = EmberClientBuilder
      .default[IO]
      .build
      .use { c =>
        val bad = Request[IO](method = POST, uri = uri"http://localhost:9993/log/logger/Nonsense")
        c.status(bad).map(s => assert(s.code == 400)) >>
          c.expect[String](Request[IO](method = POST, uri = uri"http://localhost:9993/stop"))
      }
      .delayBy(3.seconds)

    val res = guard
      .service("log-bad")
      .updateConfig(_.withHttpServer(_.withPort(port"9993")))
      .eventStream(_ => IO.sleep(10.hours))
      .map(checkJson)
      .compile
      .drain <& client
    res.unsafeRunSync()
  }

  test("7.panic history") {
    val stop = Request[IO](method = POST, uri = uri"http://localhost:9997/stop")
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
        _.withRestartPolicy(1.hour, _.fixedDelay(1.second).repeat)
          .withHttpServer(_.withPort(port"9997"))
          .withHistoryCapacity(3, 3, 3))
      .eventStream(_ => IO.raiseError(new Exception))
      .map(checkJson)
      .compile
      .drain &> client
    res.unsafeRunSync()
  }
}
