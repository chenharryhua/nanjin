package mtest.http

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import com.comcast.ip4s.*
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.http.client.middleware.{cookieBox, retry}
import io.circe.Json
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.client.Client
import org.http4s.client.middleware.Logger
import org.http4s.dsl.io.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.server.middleware.GZip
import org.http4s.server.{Router, Server}
import org.http4s.{HttpRoutes, Method, Request}
import org.scalatest.funsuite.AnyFunSuite

import java.net.CookieManager
import scala.concurrent.duration.DurationInt
import scala.util.Random

class HttpClientConfigTest extends AnyFunSuite {
  private val app: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "hello" / name => Ok(s"Hello, $name.")
    case POST -> Root / "post"        => Ok("posted")
    case GET -> Root / "timeout" / reason =>
      if (Random.nextInt(5) === 0) Ok(reason) else RequestTimeout(reason)
    case GET -> Root / "failure" => InternalServerError()
  }
  val server: Resource[IO, Server] = EmberServerBuilder
    .default[IO]
    .withHost(ipv4"0.0.0.0")
    .withPort(port"8080")
    .withHttpApp(Router("/" -> GZip(app)).orNotFound)
    .build

  val ember: Resource[IO, Client[IO]] =
    EmberClientBuilder.default[IO].build.map(Logger(logHeaders = true, logBody = true, _ => false)(_))

  test("hello, world") {
    val client = ember
    server
      .surround(client.use(_.expect[String]("http://0.0.0.0:8080/hello/world").flatMap(IO.println)))
      .unsafeRunSync()
  }

  test("timeout") {
    val client = ember.map(retry(policies.fixedRate(1.seconds).limited(2), sydneyTime))
    server
      .surround(
        client.use(c =>
          c.expect[String]("http://0.0.0.0:8080/timeout/one").attempt.flatMap(IO.println) >>
            c.expect[String]("http://0.0.0.0:8080/timeout/two").attempt.flatMap(IO.println) >>
            c.expect[String]("http://0.0.0.0:8080/timeout/three").attempt.flatMap(IO.println) >>
            c.expect[String]("http://0.0.0.0:8080/timeout/four").attempt.flatMap(IO.println) >>
            c.expect[String]("http://0.0.0.0:8080/timeout/five").attempt.flatMap(IO.println) >>
            c.expect[String]("http://0.0.0.0:8080/timeout/six").attempt.flatMap(IO.println)))
      .unsafeRunSync()
  }

  test("failure") {
    val client = ember.map(retry(policies.fixedRate(1.seconds).limited(3), sydneyTime))
    val run = server.surround(client.use(_.expect[String]("http://0.0.0.0:8080/failure").flatMap(IO.println)))
    assertThrows[Exception](run.unsafeRunSync())
  }

  test("give up") {
    val client = ember.map(retry(policies.giveUp, sydneyTime))
    val run = server.surround(client.use(_.expect[String]("http://0.0.0.0:8080/failure").flatMap(IO.println)))
    assertThrows[Exception](run.unsafeRunSync())
  }

  test("post") {
    val postRequest = Request[IO](
      method = Method.POST,
      uri = uri"http://0.0.0.0:8080/post"
    ).withEntity(
      Json.obj("a" -> Json.fromString("a"), "b" -> Json.fromInt(1))
    )
    val client = ember.map(retry(policies.giveUp, sydneyTime))
    server.surround(client.use(_.expect[String](postRequest).flatMap(IO.println))).unsafeRunSync()
  }

  test("cookie box") {
    val client = ember.map(cookieBox(new CookieManager()))
    server
      .surround(client.use(_.expect[String]("http://0.0.0.0:8080/hello/world").flatMap(IO.println)))
      .unsafeRunSync()
  }

}