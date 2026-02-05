package mtest.http

import cats.effect.*
import com.github.chenharryhua.nanjin.http.client.auth
import com.github.chenharryhua.nanjin.http.client.auth.{AuthorizationCode, ClientCredentials}
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.client.Client
import org.http4s.client.middleware.Logger
import org.http4s.dsl.io.*
import org.http4s.headers.{`Content-Type`, Authorization}
import org.http4s.implicits.http4sLiteralsSyntax

final class AuthLoginSuite extends CatsEffectSuite {

  /* -------------------------------------------------------------------------- */
  /* Test helpers                                                                */
  /* -------------------------------------------------------------------------- */

  private def tokenServer(
    expectedGrantType: String,
    accessToken: String = "token-123"
  ): Client[IO] = {
    val app = HttpApp[IO] { case req @ POST -> Root / "token" =>
      req.as[UrlForm].flatMap { form =>
        assertEquals(form.getFirst("grant_type"), Some(expectedGrantType))

        Ok(
          s"""
             |{
             |  "access_token": "$accessToken",
             |  "token_type": "Bearer",
             |  "expires_in": 3600,
             |  "id_token": "id",
             |  "refresh_token": "refresh_token"
             |}
             |""".stripMargin
        ).map(_.withContentType(`Content-Type`(MediaType.application.json)))
      }
    }

    Client.fromHttpApp(app)
  }

  private val protectedResource: Client[IO] =
    Client.fromHttpApp(
      HttpRoutes
        .of[IO] { case req =>
          req.headers.get[Authorization] match {
            case Some(_) => Ok("ok")
            case None    => Forbidden("missing auth")
          }
        }
        .orNotFound
    )

  /* -------------------------------------------------------------------------- */
  /* Client Credentials                                                          */
  /* -------------------------------------------------------------------------- */

  test("clientCredentials login injects Authorization header") {
    val authClient = Resource
      .pure[IO, Client[IO]](
        tokenServer(expectedGrantType = "client_credentials")
      )
      .map(Logger(logHeaders = true, logBody = true))

    val credential =
      ClientCredentials(
        auth_endpoint = uri"/token",
        client_id = "client-id",
        client_secret = "secret"
      )

    val login =
      auth.clientCredentials[IO](authClient, credential)

    login.loginR(protectedResource).use { authed =>
      authed.expect[String](uri"/hello").map { body =>
        assertEquals(body, "ok")
      }
    }
  }

  /* -------------------------------------------------------------------------- */
  /* Authorization Code                                                          */
  /* -------------------------------------------------------------------------- */

  test("authorizationCode login injects Authorization header") {
    val authClient = Resource
      .pure[IO, Client[IO]](
        tokenServer(expectedGrantType = "authorization_code")
      )
      .map(Logger(logHeaders = true, logBody = true))

    val credential =
      AuthorizationCode(
        auth_endpoint = uri"/token",
        client_id = "client-id",
        client_secret = "secret",
        code = "auth-code",
        redirect_uri = "https://example.com/callback"
      )

    val login =
      auth.authorizationCode[IO](authClient, credential)

    login.loginR(protectedResource).use { authed =>
      authed.expect[String](uri"/resource").map { body =>
        assertEquals(body, "ok")
      }
    }
  }

  /* -------------------------------------------------------------------------- */
  /* Sanity: token is reused within lifetime                                     */
  /* -------------------------------------------------------------------------- */

  test("login reuses token within its lifetime") {
    val ref = Ref.unsafe[IO, Int](0)

    val app = HttpApp[IO] { case POST -> Root / "token" =>
      ref.updateAndGet(_ + 1) *> Ok(
        """
          |{
          |  "access_token": "cached-token",
          |  "token_type": "Bearer",
          |  "expires_in": 3600
          |}
          |""".stripMargin
      )
    }

    val authClient =
      Resource.pure[IO, Client[IO]](Client.fromHttpApp(app)).map(Logger(logHeaders = true, logBody = true))

    val credential =
      ClientCredentials(
        auth_endpoint = uri"/token",
        client_id = "id",
        client_secret = "secret"
      )

    val login =
      auth.clientCredentials[IO](authClient, credential)

    login.loginR(protectedResource).use { authed =>
      for {
        _ <- authed.expect[String](uri"/a")
        _ <- authed.expect[String](uri"/b")
        n <- ref.get
      } yield assertEquals(n, 1)
    }
  }
}
