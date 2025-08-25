package mtest.http

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.implicits.catsSyntaxApplyOps
import com.comcast.ip4s.*
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.http.client.auth.salesforce.MarketingCloud
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

class SalesforceMarketingCloudTest extends AnyFunSuite {
  private val token = Json.obj(
    "access_token" -> "access".asJson,
    "token_type" -> "bearer".asJson,
    "expires_in" -> 3.asJson,
    "scope" -> "scope".asJson,
    "soap_instance_url" -> "http://127.0.0.1:8080".asJson,
    "rest_instance_url" -> "http://127.0.0.1:8080".asJson
  )

  private val bools = BooleanList(LazyList(false, false, true, false, true))
  private def service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case POST -> Root / "v2" / "token" =>
      if (bools.get) Ok(token) else GatewayTimeout()
    case GET -> Root / "data" => Ok("salesforce.marketing.cloud.data")
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
    .map(retry(sydneyTime, Policy.fixedDelay(2.second)))

  private val cred: MarketingCloud[IO] = MarketingCloud.rest[IO](authClient)(
    auth_endpoint = uri"http://127.0.0.1:8080",
    client_id = "a",
    client_secret = "b"
  )

  val client: Resource[IO, Client[IO]] =
    EmberClientBuilder
      .default[IO]
      .build
      .flatMap(cred.loginR)
      .map(Logger(logHeaders = true, logBody = true, _ => false))

  test("salesforce.marketing.cloud") {
    (server *> client)
      .use(c => c.expect[String]("data") >> IO.sleep(7.seconds) >> c.expect[String]("data"))
      .unsafeRunSync()
  }
}
