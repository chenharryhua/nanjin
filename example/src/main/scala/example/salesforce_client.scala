package example

import cats.effect.{IO, Resource}
import com.github.chenharryhua.nanjin.aws.ParameterStore
import com.github.chenharryhua.nanjin.common.Secret
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.http.client.auth.{Login, Salesforce}
import com.github.chenharryhua.nanjin.http.client.middleware.httpRetry
import org.http4s.client.Client
import org.http4s.client.middleware.Logger
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.syntax.all.*

import scala.concurrent.duration.DurationInt

/** Example: building an authenticated Salesforce HTTP client.
  *
  * Credentials are read from AWS Parameter Store, used to obtain a Salesforce password-grant login, and the
  * resulting OAuth token authenticates a second client. `get` shows issuing a request with it.
  *
  * The endpoint (`test.salesforce.com`), parameter names, and request path are placeholders — substitute your
  * own. Secrets are wrapped in `Secret` so they are not logged.
  */
object salesforce_client {
  // base client used only for the auth handshake: logs headers (not body) and retries with jitter
  private val authClient: Resource[IO, Client[IO]] = EmberClientBuilder
    .default[IO]
    .build
    .map(Logger(logHeaders = true, logBody = true, _ => false))
    .map(httpRetry(sydneyTime, _.fixedDelay(0.second).jitter(5.seconds)))

  // fetch the OAuth credentials from Parameter Store and assemble a password-grant login
  private val credential: Resource[IO, Login[IO]] =
    ParameterStore[IO](identity).evalMap { ps =>
      for {
        id <- ps.fetch("salesforce/client_id")
        cs <- ps.fetch("salesforce/client_secret")
        un <- ps.fetch("salesforce/username")
        pw <- ps.fetch("salesforce/password")
      } yield Salesforce.PasswordGrant(
        auth_endpoint = uri"https://test.salesforce.com",
        client_id = id.value,
        client_secret = Secret(cs.value),
        username = un.value,
        password = Secret(pw.value))
    }.flatMap(pg => Salesforce(authClient, pg))

  // a request client whose calls carry the acquired Salesforce OAuth token
  private val client: Resource[IO, Client[IO]] =
    credential.flatMap(_.login(EmberClientBuilder.default[IO].build))

  /** Issue an authenticated GET and read the response body as a string. */
  val get: IO[String] = client.use(_.expect[String]("path"))

}
