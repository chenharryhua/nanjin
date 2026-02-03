package example

import cats.effect.{IO, Resource}
import com.github.chenharryhua.nanjin.aws.ParameterStore
import com.github.chenharryhua.nanjin.common.aws.ParameterStorePath
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.http.client.auth.salesforce.Iot
import com.github.chenharryhua.nanjin.http.client.middleware.retry
import org.http4s.client.Client
import org.http4s.client.middleware.Logger
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.implicits.http4sLiteralsSyntax

import scala.concurrent.duration.DurationInt

object salesforce_client {
  private val authClient: Resource[IO, Client[IO]] = EmberClientBuilder
    .default[IO]
    .build
    .map(Logger(logHeaders = true, logBody = true, _ => false))
    .map(retry(sydneyTime, Policy.fixedDelay(0.second).jitter(5.seconds)))

  private val credential: Resource[IO, Iot[IO]] =
    ParameterStore[IO](identity).evalMap { ps =>
      for {
        id <- ps.fetch(ParameterStorePath("salesforce/client_id"))
        cs <- ps.fetch(ParameterStorePath("salesforce/client_secret"))
        un <- ps.fetch(ParameterStorePath("salesforce/username"))
        pw <- ps.fetch(ParameterStorePath("salesforce/password"))
      } yield Iot(authClient)(
        auth_endpoint = uri"https://test.salesforce.com",
        client_id = id.value,
        client_secret = cs.value,
        username = un.value,
        password = pw.value,
        expiresIn = 2.hours
      )
    }

  private val client: Resource[IO, Client[IO]] =
    credential.flatMap(_.loginR(EmberClientBuilder.default[IO].build))

  val get: IO[String] = client.use(_.expect[String]("path"))

}
