package mtest.http

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import com.comcast.ip4s.*
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.http.client.middleware.{cookieBox, retry, traceClient}
import com.github.chenharryhua.nanjin.http.server.middleware.traceServer
import io.circe.Json
import natchez.log.Log
import natchez.{EntryPoint, Span}
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.client.Client
import org.http4s.client.middleware.Logger as MLogger
import org.http4s.dsl.io.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.server.middleware.GZip
import org.http4s.server.{Router, Server}
import org.http4s.{HttpRoutes, Method, Request}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.net.CookieManager
import scala.concurrent.duration.DurationInt
import scala.util.Random

class HttpTest extends AnyFunSuite {
  implicit val log: Logger[IO] = Slf4jLogger.getLoggerFromName("logger")
  private val entryPoint: EntryPoint[IO] = Log.entryPoint[IO]("http-test")

  private def service(span: Span[IO]): HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "trace" / name     => span.log("trace") >> Ok(s"Hello, $name.")
    case GET -> Root / "cookie"           => Ok("cookie")
    case POST -> Root / "post"            => Ok("posted")
    case GET -> Root / "timeout" / reason =>
      if (Random.nextInt(5) === 0) Ok(reason) else RequestTimeout(reason)
    case GET -> Root / "failure" => InternalServerError()
  }

  val server: Resource[IO, Server] = EmberServerBuilder
    .default[IO]
    .withHost(ipv4"0.0.0.0")
    .withPort(port"8080")
    .withHttpApp(Router("/" -> GZip(traceServer(entryPoint)(service))).orNotFound)
    .build

  val ember: Resource[IO, Client[IO]] =
    EmberClientBuilder.default[IO].build.map(MLogger(logHeaders = true, logBody = true)(_))

  test("timeout") {
    val client = ember.map(retry(Policy.fixedRate(1.seconds).limited(2), sydneyTime))
    server
      .surround(
        client.use(c =>
          c.expect[String]("http://127.0.0.1:8080/timeout/one").attempt.flatMap(IO.println) >>
            c.expect[String]("http://127.0.0.1:8080/timeout/two").attempt.flatMap(IO.println) >>
            c.expect[String]("http://127.0.0.1:8080/timeout/three").attempt.flatMap(IO.println) >>
            c.expect[String]("http://127.0.0.1:8080/timeout/four").attempt.flatMap(IO.println) >>
            c.expect[String]("http://127.0.0.1:8080/timeout/five").attempt.flatMap(IO.println) >>
            c.expect[String]("http://127.0.0.1:8080/timeout/six").attempt.flatMap(IO.println)))
      .unsafeRunSync()
  }

  test("failure") {
    val client = ember.map(retry(Policy.fixedRate(1.seconds).limited(3), sydneyTime))
    val run =
      server.surround(client.use(_.expect[String]("http://127.0.0.1:8080/failure").flatMap(IO.println)))
    assertThrows[Exception](run.unsafeRunSync())
  }

  test("give up") {
    val client = ember.map(retry(Policy.giveUp, sydneyTime))
    val run =
      server.surround(client.use(_.expect[String]("http://127.0.0.1:8080/failure").flatMap(IO.println)))
    assertThrows[Exception](run.unsafeRunSync())
  }

  test("post") {
    val postRequest = Request[IO](
      method = Method.POST,
      uri = uri"http://127.0.0.1:8080/post"
    ).withEntity(
      Json.obj("a" -> Json.fromString("a"), "b" -> Json.fromInt(1))
    )
    val client = ember.map(retry(Policy.giveUp, sydneyTime))
    server.surround(client.use(_.expect[String](postRequest).flatMap(IO.println))).unsafeRunSync()
  }

  test("cookie box") {
    val client = ember.map(cookieBox(new CookieManager()))
    server
      .surround(client.use(_.expect[String]("http://127.0.0.1:8080/cookie").flatMap(IO.println)))
      .unsafeRunSync()
  }

  test("trace") {
    val client = entryPoint.root("root").flatMap(ep => ember.map(traceClient(ep)))
    server
      .surround(client.use(_.expect[String]("http://127.0.0.1:8080/trace/world").flatMap(IO.println)))
      .unsafeRunSync()
  }

}
