package mtest.http

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.implicits.catsSyntaxApplyOps
import com.comcast.ip4s.*
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.http.client.auth.salesforce.Iot
import com.github.chenharryhua.nanjin.http.client.middleware.retry
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.client.Client
import org.http4s.client.middleware.Logger
import org.http4s.dsl.io.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.server.{Router, Server}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class SalesforceIotTest extends AnyFunSuite {
  private val token = Json.obj(
    "access_token" -> "access".asJson,
    "instance_url" -> "http://127.0.0.1:8080".asJson,
    "id" -> "id-abc".asJson,
    "token_type" -> "bearer".asJson,
    "issued_at" -> "2012/1/1".asJson,
    "signature" -> "signature".asJson
  )

  private val bools = BooleanList(LazyList(false, false, true))
  private def service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case POST -> Root / "services" / "oauth2" / "token" =>
      if (bools.get) Ok(token) else GatewayTimeout()
    case GET -> Root / "data" => Ok("salesforce.iot.data")
  }

  val server: Resource[IO, Server] = EmberServerBuilder
    .default[IO]
    .withHost(ipv4"0.0.0.0")
    .withPort(port"8080")
    .withHttpApp(Router("/" -> service).orNotFound)
    .build

  private val authClient: Resource[IO, Client[IO]] = EmberClientBuilder
    .default[IO]
    .build
    .map(Logger(logHeaders = true, logBody = true, _ => false))
    .map(retry(sydneyTime, Policy.fixedDelay(0.second).jitter(3.seconds)))

  val cred: Iot[IO] = Iot(authClient)(
    auth_endpoint = uri"http://127.0.0.1:8080",
    client_id = "a",
    client_secret = "b",
    username = "c",
    password = "d",
    expiresIn = 2.hours
  )

  val client: Resource[IO, Client[IO]] =
    EmberClientBuilder
      .default[IO]
      .build
      .flatMap(cred.loginR)
      .map(Logger(logHeaders = true, logBody = true, _ => false))

  test("salesforce.iot") {
    (server *> client).use(_.expect[String]("data")).unsafeRunSync()
  }
}
