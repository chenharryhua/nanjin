package mtest.http

import cats.effect.{IO, Resource}
import com.comcast.ip4s.*
import com.github.chenharryhua.nanjin.common.Secret
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.http.client.auth
import com.github.chenharryhua.nanjin.http.client.auth.{AuthorizationCode, ClientCredentials, Login}
import com.github.chenharryhua.nanjin.http.client.middleware.recklessHttpRetry
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.client.Client
import org.http4s.dsl.io.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.*
import org.http4s.server.Router
import org.http4s.{HttpRoutes, Uri}

import scala.concurrent.duration.DurationInt

/** Exercises the OAuth 2.0 Client Credentials and Authorization Code flows against a fake token endpoint that
  * fails twice before succeeding, so the token-fetch retry, token attachment, and a protected-resource call
  * are all covered without real infrastructure. The server binds to an ephemeral port (`port"0"`) and its
  * actual address is read back to build the URIs, so the test is parallel-safe and never clashes on a fixed
  * port.
  */
class OAuth2Test extends CatsEffectSuite {

  // Client Credentials token payload: token_type/access_token, optional expires_in and refresh_token.
  private val clientCredentialsToken = Json.obj(
    "token_type" -> "bearer".asJson,
    "access_token" -> "cc-access".asJson,
    "expires_in" -> 3600L.asJson
  )

  // Authorization Code token payload: all fields required, expires_in is a plain Long.
  private val authorizationCodeToken = Json.obj(
    "access_token" -> "ac-access".asJson,
    "refresh_token" -> "ac-refresh".asJson,
    "id_token" -> "ac-id".asJson,
    "token_type" -> "bearer".asJson,
    "expires_in" -> 3600L.asJson
  )

  private def service(token: Json, flaky: BooleanList): HttpRoutes[IO] = HttpRoutes.of[IO] {
    case POST -> Root / "oauth2" / "token" =>
      if (flaky.get) Ok(token) else GatewayTimeout()
    case GET -> Root / "data" => Ok("protected.data")
  }

  /** Starts the fake auth+resource server on an ephemeral port and yields its base URI (`http://host:port`).
    */
  private def baseUri(token: Json, flaky: BooleanList): Resource[IO, Uri] =
    EmberServerBuilder
      .default[IO]
      .withHost(ipv4"127.0.0.1")
      .withPort(port"0")
      .withHttpApp(Router("/" -> service(token, flaky)).orNotFound)
      .build
      .map(server => Uri.unsafeFromString(s"http://${server.address.getHostName}:${server.address.getPort}"))

  // token endpoint is a POST, so retry regardless of method to survive the transient 504s
  private def authClient: Resource[IO, Client[IO]] =
    EmberClientBuilder
      .default[IO]
      .build
      .map(recklessHttpRetry(sydneyTime, _.fixedDelay(1.second).repeat))

  test("1.client credentials flow attaches the token and reaches the protected resource") {
    val flaky = BooleanList(LazyList(false, false, true))
    val program = for {
      base <- baseUri(clientCredentialsToken, flaky)
      credential = ClientCredentials(
        auth_endpoint = base / "oauth2" / "token",
        client_id = "id",
        client_secret = Secret("secret"))
      login: Login[IO] = auth.clientCredentials(authClient, credential)
      client <- EmberClientBuilder.default[IO].build.flatMap(login.login)
    } yield (client, base)

    program.use { case (client, base) =>
      client.expect[String](base / "data").map(data => assertEquals(data, "\"protected.data\""))
    }
  }

  test("2.authorization code flow attaches the token and reaches the protected resource") {
    val flaky = BooleanList(LazyList(false, false, true))
    val program = for {
      base <- baseUri(authorizationCodeToken, flaky)
      credential = AuthorizationCode(
        auth_endpoint = base / "oauth2" / "token",
        client_id = "id",
        client_secret = Secret("secret"),
        code = Secret("auth-code"),
        redirect_uri = "http://127.0.0.1/callback")
      login: Login[IO] = auth.authorizationCode(authClient, credential)
      client <- EmberClientBuilder.default[IO].build.flatMap(login.login)
    } yield (client, base)

    program.use { case (client, base) =>
      client.expect[String](base / "data").map(data => assertEquals(data, "\"protected.data\""))
    }
  }
}
