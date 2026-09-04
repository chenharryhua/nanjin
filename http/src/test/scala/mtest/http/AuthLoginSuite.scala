package mtest.http

import cats.data.NonEmptyList
import cats.effect.*
import cats.syntax.parallel.given
import com.github.chenharryhua.nanjin.common.Secret
import com.github.chenharryhua.nanjin.http.client.auth
import com.github.chenharryhua.nanjin.http.client.auth.{
  AuthorizationCode,
  ClientCredentials,
  Salesforce,
  UriJsonCodec
}
import io.circe.syntax.given
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.client.Client
import org.http4s.client.middleware.Logger
import org.http4s.dsl.io.*
import org.http4s.headers.{`Content-Type`, Authorization}
import org.http4s.implicits.*

import scala.concurrent.duration.*

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
        client_secret = Secret("secret")
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
        client_secret = Secret("secret"),
        code = Secret("auth-code"),
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
        client_secret = Secret("secret")
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
        client_secret = Secret("secret")
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

  test("4a.unauthorized response is released exactly once before retry") {
    val tokenCalls = Ref.unsafe[IO, Int](0)
    val unauthorizedReleases = Ref.unsafe[IO, Int](0)

    val authClient = Resource.pure[IO, Client[IO]](
      Client.fromHttpApp(HttpApp[IO] {
        case POST -> Root / "token" =>
          tokenCalls.updateAndGet(_ + 1).flatMap { call =>
            val token = if (call == 1) "old-token" else "new-token"
            Ok(
              s"""
                 |{
                 |  "access_token": "$token",
                 |  "token_type": "Bearer",
                 |  "expires_in": 3600
                 |}
                 |""".stripMargin
            )
          }
        case _ => InternalServerError()
      })
    )

    val resourceClient = Client[IO] { request =>
      request.headers.get[Authorization] match {
        case Some(header) if header.value == "Bearer old-token" =>
          Resource.make(IO.pure(Response[IO](Status.Unauthorized)))(_ => unauthorizedReleases.update(_ + 1))
        case Some(header) if header.value == "Bearer new-token" =>
          Resource.pure(Response[IO](Status.Ok))
        case _ => Resource.pure(Response[IO](Status.Forbidden))
      }
    }

    val credential = ClientCredentials(
      auth_endpoint = uri"/token",
      client_id = "id",
      client_secret = Secret("secret")
    )

    auth.clientCredentials[IO](authClient, credential).flatMap(_.login(resourceClient)).use { authed =>
      authed.run(Request[IO](Method.GET, uri"/resource")).use { response =>
        IO(assertEquals(response.status, Status.Ok))
      }
    } *> unauthorizedReleases.get.map(releases => assertEquals(releases, 1))
  }

  test("4b.cancellation during first request releases the connection") {
    val released = Ref.unsafe[IO, Boolean](false)

    val authClient = Resource.pure[IO, Client[IO]](
      Client.fromHttpApp(HttpApp[IO] {
        case POST -> Root / "token" =>
          Ok("""{"access_token":"t","token_type":"Bearer","expires_in":3600}""")
        case _ => InternalServerError()
      })
    )

    // A client whose response is acquired successfully but tracks release
    val slowClient = Client[IO] { _ =>
      Resource.make(IO.pure(Response[IO](Status.Ok)))(_ => released.set(true))
    }

    val credential = ClientCredentials(
      auth_endpoint = uri"/token",
      client_id = "id",
      client_secret = Secret("secret")
    )

    auth.clientCredentials[IO](authClient, credential).flatMap(_.login(slowClient)).use { authed =>
      for {
        fiber <- authed.run(Request[IO](Method.GET, uri"/resource")).surround(IO.never[Unit]).start
        _ <- IO.sleep(50.millis)
        _ <- fiber.cancel
        r <- released.get
      } yield assert(r, "response resource should be released on cancellation")
    }
  }

  test("4c.second 401 after refresh is returned to caller without looping") {
    val tokenCalls = Ref.unsafe[IO, Int](0)

    val authClient = Resource.pure[IO, Client[IO]](
      Client.fromHttpApp(HttpApp[IO] {
        case POST -> Root / "token" =>
          tokenCalls.updateAndGet(_ + 1).flatMap { n =>
            Ok(s"""{"access_token":"token-$n","token_type":"Bearer","expires_in":3600}""")
          }
        case _ => InternalServerError()
      })
    )

    // Always returns 401 regardless of token
    val alwaysUnauthorized = Client.fromHttpApp(HttpApp[IO] { _ =>
      IO.pure(Response[IO](Status.Unauthorized))
    })

    val credential = ClientCredentials(
      auth_endpoint = uri"/token",
      client_id = "id",
      client_secret = Secret("secret")
    )

    auth.clientCredentials[IO](authClient, credential).flatMap(_.login(alwaysUnauthorized)).use { authed =>
      authed.run(Request[IO](Method.GET, uri"/resource")).use { response =>
        tokenCalls.get.map { n =>
          // Should see initial token fetch + one refresh on 401, then the second 401 is returned
          assertEquals(response.status, Status.Unauthorized)
          assertEquals(n, 2)
        }
      }
    }
  }

  test("4d.getToken failure during 401 recovery propagates the error") {
    val tokenCalls = Ref.unsafe[IO, Int](0)

    val authClient = Resource.pure[IO, Client[IO]](
      Client.fromHttpApp(HttpApp[IO] {
        case POST -> Root / "token" =>
          tokenCalls.updateAndGet(_ + 1).flatMap { n =>
            if (n == 1) Ok("""{"access_token":"old","token_type":"Bearer","expires_in":3600}""")
            else InternalServerError("token server down")
          }
        case _ => InternalServerError()
      })
    )

    val resourceClient = Client.fromHttpApp(HttpApp[IO] { _ =>
      IO.pure(Response[IO](Status.Unauthorized))
    })

    val credential = ClientCredentials(
      auth_endpoint = uri"/token",
      client_id = "id",
      client_secret = Secret("secret")
    )

    auth.clientCredentials[IO](authClient, credential).flatMap(_.login(resourceClient)).use { authed =>
      authed.run(Request[IO](Method.GET, uri"/resource")).use_.attempt.map { result =>
        assert(result.isLeft, "should propagate the token fetch error")
      }
    }
  }

  test("5.Uri JSON codec round-trips HTTP4S URIs") {
    import UriJsonCodec.given

    val uri = uri"https://example.com/api"
    val json = uri.asJson
    val decoded = json.as[Uri]

    assertEquals(json.noSpaces, "\"https://example.com/api\"")
    assertEquals(decoded, Right(uri))
  }

  test("6.Uri JSON codec preserves explicit ports") {
    import UriJsonCodec.given

    val uri = uri"https://example.com:8443/api"
    val json = uri.asJson
    val decoded = json.as[Uri]

    assertEquals(json.noSpaces, "\"https://example.com:8443/api\"")
    assertEquals(decoded, Right(uri))
  }

  test("7.Salesforce password grant rewrites the request URI") {
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
      client_secret = Secret("secret"),
      username = "user",
      password = Secret("pass")
    )

    Salesforce[IO](authClient, credential).flatMap(_.login(Client.fromHttpApp(resourceApp))).use { authed =>
      authed.expect[String](uri"/resource")
    }
  }

  test("8.Salesforce password grant preserves query string") {
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
        case Some(_) =>
          assertEquals(req.uri.host.map(_.value), Some("example.my.salesforce.com"))
          assertEquals(req.uri.query.params.get("q"), Some("SELECT Id FROM Account"))
          assertEquals(req.uri.path.renderString, "/services/data/v58.0/query")
          Ok("ok")
        case _ => Forbidden("missing auth")
      }
    }

    val authClient = Resource.pure[IO, Client[IO]](Client.fromHttpApp(authApp))
    val credential = Salesforce.PasswordGrant(
      auth_endpoint = uri"/token",
      client_id = "client-id",
      client_secret = Secret("secret"),
      username = "user",
      password = Secret("pass")
    )

    Salesforce[IO](authClient, credential).flatMap(_.login(Client.fromHttpApp(resourceApp))).use { authed =>
      authed.expect[String](Uri.unsafeFromString("/services/data/v58.0/query?q=SELECT+Id+FROM+Account"))
    }
  }

  test("9.clientCredentials with scopes includes scope in token request") {
    val scopeReceived = Ref.unsafe[IO, Option[String]](None)

    val app = HttpApp[IO] {
      case req @ POST -> Root / "token" =>
        req.as[UrlForm].flatMap { form =>
          scopeReceived.set(form.getFirst("scope")) *> Ok(
            """
              |{
              |  "access_token": "scoped-token",
              |  "token_type": "Bearer",
              |  "expires_in": 3600
              |}
              |""".stripMargin
          )
        }
      case _ => InternalServerError()
    }

    val authClient = Resource.pure[IO, Client[IO]](Client.fromHttpApp(app))
    val credential = ClientCredentials(
      auth_endpoint = uri"/token",
      client_id = "id",
      client_secret = Secret("secret"),
      scope = Some(NonEmptyList.of("read", "write"))
    )

    auth.clientCredentials[IO](authClient, credential).flatMap(_.login(protectedResource)).use { authed =>
      for {
        _ <- authed.expect[String](uri"/data")
        s <- scopeReceived.get
      } yield assertEquals(s, Some("read write"))
    }
  }

  test("10.clientCredentials with refresh_token uses refresh on renewal") {
    val tokenCalls = Ref.unsafe[IO, Int](0)

    val app = HttpApp[IO] {
      case req @ POST -> Root / "token" =>
        req.as[UrlForm].flatMap { form =>
          tokenCalls.updateAndGet(_ + 1).flatMap { n =>
            val grantType = form.getFirst("grant_type")
            if (n == 1) {
              assertEquals(grantType, Some("client_credentials"))
              Ok(
                """
                  |{
                  |  "access_token": "token-1",
                  |  "token_type": "Bearer",
                  |  "expires_in": 1,
                  |  "refresh_token": "refresh-abc"
                  |}
                  |""".stripMargin
              )
            } else {
              assertEquals(grantType, Some("refresh_token"))
              assertEquals(form.getFirst("refresh_token"), Some("refresh-abc"))
              Ok(
                """
                  |{
                  |  "access_token": "token-2",
                  |  "token_type": "Bearer",
                  |  "expires_in": 3600
                  |}
                  |""".stripMargin
              )
            }
          }
        }
      case _ => InternalServerError()
    }

    val authClient = Resource.pure[IO, Client[IO]](Client.fromHttpApp(app))
    val credential = ClientCredentials(
      auth_endpoint = uri"/token",
      client_id = "id",
      client_secret = Secret("secret")
    )

    auth.clientCredentials[IO](authClient, credential).flatMap(_.login(protectedResource)).use { authed =>
      for {
        _ <- authed.expect[String](uri"/a")
        // expires_in=1 is shorter than the renewal skew, so the scheduled delay is clamped to
        // the renewMinDelay floor (5s) rather than firing immediately. Wait past that floor.
        _ <- IO.sleep(6.seconds)
        _ <- authed.expect[String](uri"/b")
        n <- tokenCalls.get
      } yield assert(n >= 2)
    }
  }

  test("10a.failing renewal backs off instead of busy-looping the auth endpoint") {
    // First fetch succeeds with a short-lived token; every subsequent renewal fails. Without a
    // backoff floor the loop would spin `foreverM` with no delay and hammer the endpoint. With the
    // fix, renewal attempts over a fixed window are bounded by renewFailureBackoff (~5s).
    val tokenCalls = Ref.unsafe[IO, Int](0)

    val app = HttpApp[IO] {
      case POST -> Root / "token" =>
        tokenCalls.updateAndGet(_ + 1).flatMap { n =>
          if (n == 1)
            Ok("""{"access_token":"t1","token_type":"Bearer","expires_in":1}""")
          else
            InternalServerError("renewal boom")
        }
      case _ => InternalServerError()
    }

    val authClient = Resource.pure[IO, Client[IO]](Client.fromHttpApp(app))
    val credential = ClientCredentials(
      auth_endpoint = uri"/token",
      client_id = "id",
      client_secret = Secret("secret")
    )

    auth.clientCredentials[IO](authClient, credential).flatMap(_.login(protectedResource)).use { authed =>
      for {
        _ <- authed.expect[String](uri"/a")
        _ <- IO.sleep(3.seconds) // shorter than the renewFailureBackoff floor
        n <- tokenCalls.get
      } yield
        // initial fetch (1) + at most a couple of backed-off renewal attempts.
        // A busy loop would produce hundreds/thousands here.
        assert(n <= 3, s"expected the renewal loop to back off, but it made $n token calls")
    }
  }

  test("11.concurrent 401s use SingleFlight to deduplicate token refresh") {
    val tokenCalls = Ref.unsafe[IO, Int](0)
    val requestCount = Ref.unsafe[IO, Int](0)

    val authApp = HttpApp[IO] {
      case POST -> Root / "token" =>
        tokenCalls.updateAndGet(_ + 1).flatMap { n =>
          IO.sleep(100.millis) *> Ok(
            s"""
               |{
               |  "access_token": "token-$n",
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
          requestCount.updateAndGet(_ + 1).flatMap { _ =>
            val token = authHeader.value.stripPrefix("Bearer ")
            // First token always triggers 401
            if (token == "token-1") IO.pure(Response[IO](Status.Unauthorized))
            else Ok("ok")
          }
        case _ => Forbidden("missing auth")
      }
    }

    val authClient = Resource.pure[IO, Client[IO]](Client.fromHttpApp(authApp))
    val credential = ClientCredentials(
      auth_endpoint = uri"/token",
      client_id = "id",
      client_secret = Secret("secret")
    )

    auth.clientCredentials[IO](authClient, credential).flatMap(_.login(Client.fromHttpApp(resourceApp))).use {
      authed =>
        // Fire 5 concurrent requests — all should hit 401 on first token, but only one refresh should happen
        val requests = List.fill(5)(authed.expect[String](uri"/data"))
        requests.parSequence.flatMap { results =>
          tokenCalls.get.map { n =>
            results.foreach(r => assertEquals(r, "ok"))
            // Initial fetch + exactly one refresh via SingleFlight = 2
            assertEquals(n, 2)
          }
        }
    }
  }

  test("12.Login.login(Resource) convenience method works") {
    val authClient = Resource.pure[IO, Client[IO]](
      tokenServer(expectedGrantType = "client_credentials")
    )
    val credential = ClientCredentials(
      auth_endpoint = uri"/token",
      client_id = "id",
      client_secret = Secret("secret")
    )

    val clientResource = Resource.pure[IO, Client[IO]](protectedResource)

    auth.clientCredentials[IO](authClient, credential).flatMap(_.login(clientResource)).use { authed =>
      authed.expect[String](uri"/hello").map(body => assertEquals(body, "ok"))
    }
  }
}
