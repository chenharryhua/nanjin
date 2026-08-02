package mtest.http

import cats.effect.*
import com.github.chenharryhua.nanjin.http.client.auth
import com.github.chenharryhua.nanjin.http.client.auth.{AuthorizationCode, ClientCredentials, Salesforce}
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.client.Client
import org.http4s.client.middleware.Logger
import org.http4s.dsl.io.*
import org.http4s.headers.{Authorization, `Content-Type`}
import org.http4s.implicits.*

final class AuthLoginSuite extends CatsEffectSuite {

  /* -------------------------------------------------------------------------- */
  /* Test helpers                                                                */
  /* -------------------------------------------------------------------------- */

  private def tokenServer(
    expectedGrantType: String,
    accessToken: String = "token-123"
  ): Client[IO] = {
    val app = HttpApp[IO] {
      case req @ POST -> Root / "token" =>
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
      case _ => InternalServerError()
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

  test("1.clientCredentials login injects Authorization header") {
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

    login.flatMap(_.login(protectedResource)).use { authed =>
      authed.expect[String](uri"/hello").map { body =>
        assertEquals(body, "ok")
      }
    }
  }

  /* -------------------------------------------------------------------------- */
  /* Authorization Code                                                          */
  /* -------------------------------------------------------------------------- */

  test("2.authorizationCode login injects Authorization header") {
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

    login.flatMap(_.login(protectedResource)).use { authed =>
      authed.expect[String](uri"/resource").map { body =>
        assertEquals(body, "ok")
      }
    }
  }

  /* -------------------------------------------------------------------------- */
  /* Sanity: token is reused within lifetime                                     */
  /* -------------------------------------------------------------------------- */

  test("3.login reuses token within its lifetime") {
    val ref = Ref.unsafe[IO, Int](0)

    val app = HttpApp[IO] {
      case POST -> Root / "token" =>
        ref.updateAndGet(_ + 1) *> Ok(
          """
            |{
            |  "access_token": "cached-token",
            |  "token_type": "Bearer",
            |  "expires_in": 3600
            |}
            |""".stripMargin
        )
      case _ => InternalServerError()
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

    login.flatMap(_.login(protectedResource)).use { authed =>
      for {
        _ <- authed.expect[String](uri"/a")
        _ <- authed.expect[String](uri"/b")
        n <- ref.get
      } yield assertEquals(n, 1)
    }
  }

  test("4.unauthorized response triggers a token refresh") {
    val tokenCalls = Ref.unsafe[IO, Int](0)
    val currentToken = Ref.unsafe[IO, String]("old-token")

    val authApp = HttpApp[IO] {
      case POST -> Root / "token" =>
        tokenCalls.updateAndGet(_ + 1).flatMap { n =>
          val nextToken = if (n == 1) "old-token" else "new-token"
          currentToken.set(nextToken) *> Ok(
            s"""
               |{
               |  "access_token": "$nextToken",
               |  "token_type": "Bearer",
               |  "expires_in": 3600
               |}
               |""".stripMargin
          )
        }
      case _ => InternalServerError()
    }

    val resourceApp = HttpApp[IO] { req =>
      req.headers.get[Authorization] match {
        case Some(authHeader) =>
          val token = authHeader.value.stripPrefix("Bearer ")
          if (token == "old-token") IO.pure(Response[IO](Status.Unauthorized)) else Ok("ok")
        case _ => Forbidden("missing auth")
      }
    }

    val authClient = Resource.pure[IO, Client[IO]](Client.fromHttpApp(authApp))
    val resourceClient = Client.fromHttpApp(resourceApp)

    val credential =
      ClientCredentials(
        auth_endpoint = uri"/token",
        client_id = "id",
        client_secret = "secret"
      )

    auth.clientCredentials[IO](authClient, credential).flatMap(_.login(resourceClient)).use { authed =>
      authed.expect[String](uri"/resource").flatMap { body =>
        for {
          n <- tokenCalls.get
          token <- currentToken.get
        } yield {
          assertEquals(body, "ok")
          assertEquals(n, 2)
          assertEquals(token, "new-token")
        }
      }
    }
  }

  test("5.Salesforce password grant rewrites the request URI") {
    val authApp = HttpApp[IO] {
      case POST -> Root / "token" =>
        Ok(
          """
            |{
            |  "access_token": "sf-token",
            |  "instance_url": "https://example.my.salesforce.com",
            |  "id": "id",
            |  "token_type": "Bearer",
            |  "issued_at": "0",
            |  "signature": "sig"
            |}
            |""".stripMargin
        )
      case _ => InternalServerError()
    }

    val resourceApp = HttpApp[IO] { req =>
      req.headers.get[Authorization] match {
        case Some(authHeader) =>
          val token = authHeader.value.stripPrefix("Bearer ")
          assertEquals(token, "sf-token")
          assertEquals(req.uri.host.map(_.value), Some("example.my.salesforce.com"))
          Ok("ok")
        case _ => Forbidden("missing auth")
      }
    }

    val authClient = Resource.pure[IO, Client[IO]](Client.fromHttpApp(authApp))
    val credential = Salesforce.PasswordGrant(
      auth_endpoint = uri"/token",
      client_id = "client-id",
      client_secret = "secret",
      username = "user",
      password = "pass"
    )

    Salesforce[IO](authClient, credential).flatMap(_.login(Client.fromHttpApp(resourceApp))).use { authed =>
      authed.expect[String](uri"/resource")
    }
  }
}
