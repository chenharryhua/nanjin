package mtest.http

import cats.data.NonEmptyList
import cats.effect.*
import cats.syntax.parallel.given
import cats.syntax.foldable.given
import com.github.chenharryhua.nanjin.common.Secret
import com.github.chenharryhua.nanjin.http.client.auth
import com.github.chenharryhua.nanjin.http.client.auth.{
  AuthorizationCode,
  ClientCredentials,
  Login,
  Salesforce,
  UriJsonCodec
}
import io.circe.syntax.given
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.client.Client
import org.http4s.client.middleware.Logger
import org.http4s.dsl.io.*
import org.http4s.headers.{`Content-Type`, Authorization, Host}
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

    login.login(protectedResource).use { authed =>
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

    login.login(protectedResource).use { authed =>
      authed.expect[String](uri"/resource").map { body =>
        assertEquals(body, "ok")
      }
    }
  }

  test("2a.authorizationCode refreshes instead of reusing a rejected authorization code") {
    val grant_types = Ref.unsafe[IO, List[String]](Nil)
    val auth_client = Resource.pure[IO, Client[IO]](
      Client.fromHttpApp(HttpApp[IO] {
        case req @ POST -> Root / "token" =>
          req.as[UrlForm].flatMap { form =>
            val grant_type = form.getFirst("grant_type").getOrElse(fail("missing grant_type"))
            grant_types.update(_ :+ grant_type) *> (grant_type match {
              case "authorization_code" =>
                Ok("""{"access_token":"old-token","refresh_token":"refresh-1","id_token":"id-1","token_type":"Bearer","expires_in":3600}""")
              case "refresh_token" =>
                assertEquals(form.getFirst("refresh_token"), Some("refresh-1"))
                Ok("""{"access_token":"new-token","refresh_token":"refresh-2","id_token":"id-2","token_type":"Bearer","expires_in":3600}""")
              case unexpected =>
                fail(s"unexpected grant_type: $unexpected")
            })
          }
        case _ => InternalServerError()
      })
    )

    val resource_client = Client.fromHttpApp(HttpApp[IO] { request =>
      request.headers.get[Authorization] match {
        case Some(header) if header.value == "Bearer old-token" =>
          IO.pure(Response[IO](Status.Unauthorized))
        case Some(header) if header.value == "Bearer new-token" => Ok("ok")
        case _                                                  => Forbidden()
      }
    })

    val credential = AuthorizationCode(
      auth_endpoint = uri"/token",
      client_id = "client-id",
      client_secret = Secret("secret"),
      code = Secret("auth-code"),
      redirect_uri = "https://example.com/callback"
    )

    auth.authorizationCode[IO](auth_client, credential).login(resource_client).use { authed =>
      for {
        body <- authed.expect[String](uri"/resource")
        grants <- grant_types.get
      } yield {
        assertEquals(body, "ok")
        assertEquals(grants, List("authorization_code", "refresh_token"))
      }
    }
  }

  test("2b.authorizationCode accepts a minimal token response and sends the exact exchange") {
    val token_calls = Ref.unsafe[IO, Int](0)
    val auth_client = Resource.pure[IO, Client[IO]](
      Client.fromHttpApp(HttpApp[IO] {
        case request @ POST -> Root / "token" =>
          request.as[UrlForm].flatMap { form =>
            assertEquals(form.getFirst("grant_type"), Some("authorization_code"))
            assertEquals(form.getFirst("client_id"), Some("client-id"))
            assertEquals(form.getFirst("code"), Some("auth-code"))
            assertEquals(form.getFirst("redirect_uri"), Some("https://example.com/callback"))
            assertEquals(form.getFirst("scope"), None)
            assertEquals(
              request.headers.get[Authorization].map(_.value),
              Some(Authorization(BasicCredentials("client-id", "secret")).value)
            )
            token_calls.update(_ + 1) *>
              Ok("""{"access_token":"minimal-token","token_type":"Bearer"}""")
          }
        case _ => InternalServerError()
      })
    )

    val credential = AuthorizationCode(
      auth_endpoint = uri"/token",
      client_id = "client-id",
      client_secret = Secret("secret"),
      code = Secret("auth-code"),
      redirect_uri = "https://example.com/callback"
    )

    auth.authorizationCode[IO](auth_client, credential).login(protectedResource).use { authed =>
      authed.expect[String](uri"/resource")
    }.flatMap { body =>
      token_calls.get.map { calls =>
        assertEquals(body, "ok")
        assertEquals(calls, 1)
      }
    }
  }

  test("2c.authorizationCode retains a refresh token when a refresh response omits it") {
    val refresh_tokens = Ref.unsafe[IO, List[String]](Nil)
    val token_calls = Ref.unsafe[IO, Int](0)
    val auth_client = Resource.pure[IO, Client[IO]](
      Client.fromHttpApp(HttpApp[IO] {
        case request @ POST -> Root / "token" =>
          request.as[UrlForm].flatMap { form =>
            token_calls.updateAndGet(_ + 1).flatMap {
              case 1 =>
                Ok("""{"access_token":"old-token","token_type":"Bearer","refresh_token":"refresh-1"}""")
              case 2 =>
                refresh_tokens.update(_ :+ form.getFirst("refresh_token").getOrElse("missing")) *>
                  Ok("""{"access_token":"middle-token","token_type":"Bearer"}""")
              case 3 =>
                refresh_tokens.update(_ :+ form.getFirst("refresh_token").getOrElse("missing")) *>
                  Ok("""{"access_token":"new-token","token_type":"Bearer"}""")
              case unexpected =>
                InternalServerError(s"unexpected token call: $unexpected")
            }
          }
        case _ => InternalServerError()
      })
    )
    val resource_client = Client.fromHttpApp(HttpApp[IO] { request =>
      request.headers.get[Authorization].map(_.value) match {
        case Some("Bearer old-token")    => IO.pure(Response[IO](Status.Unauthorized))
        case Some("Bearer middle-token") => IO.pure(Response[IO](Status.Unauthorized))
        case Some("Bearer new-token")    => Ok("ok")
        case _                           => Forbidden()
      }
    })
    val credential = AuthorizationCode(
      auth_endpoint = uri"/token",
      client_id = "client-id",
      client_secret = Secret("secret"),
      code = Secret("auth-code"),
      redirect_uri = "https://example.com/callback"
    )

    auth.authorizationCode[IO](auth_client, credential).login(resource_client).use { authed =>
      for {
        first_status <- authed.status(Request[IO](uri = uri"/resource"))
        body <- authed.expect[String](uri"/resource")
        used_refresh_tokens <- refresh_tokens.get
      } yield {
        assertEquals(first_status, Status.Unauthorized)
        assertEquals(body, "ok")
        assertEquals(used_refresh_tokens, List("refresh-1", "refresh-1"))
      }
    }
  }

  test("2d.authorizationCode requires reauthorization when a rejected token has no refresh token") {
    val auth_client = Resource.pure[IO, Client[IO]](
      Client.fromHttpApp(HttpApp[IO] {
        case POST -> Root / "token" =>
          Ok("""{"access_token":"token","token_type":"Bearer"}""")
        case _ => InternalServerError()
      })
    )
    val always_unauthorized = Client.fromHttpApp(HttpApp[IO](_ => IO.pure(Response[IO](Status.Unauthorized))))
    val credential = AuthorizationCode(
      auth_endpoint = uri"/token",
      client_id = "client-id",
      client_secret = Secret("secret"),
      code = Secret("auth-code"),
      redirect_uri = "https://example.com/callback"
    )

    auth.authorizationCode[IO](auth_client, credential).login(always_unauthorized).use { authed =>
      authed.status(Request[IO](uri = uri"/resource"))
    }.attempt.map {
      case Left(error) =>
        assertEquals(
          error.getMessage,
          "authorization code token has no refresh_token; user reauthorization is required")
      case Right(status) => fail(s"expected reauthorization failure, received $status")
    }
  }

  test("2e.authorizationCode login permits only one resource acquisition") {
    val token_calls = Ref.unsafe[IO, Int](0)
    val auth_client = Resource.pure[IO, Client[IO]](
      Client.fromHttpApp(HttpApp[IO] {
        case POST -> Root / "token" =>
          token_calls.update(_ + 1) *>
            Ok("""{"access_token":"token","token_type":"Bearer"}""")
        case _ => InternalServerError()
      })
    )
    val credential = AuthorizationCode(
      auth_endpoint = uri"/token",
      client_id = "client-id",
      client_secret = Secret("secret"),
      code = Secret("auth-code"),
      redirect_uri = "https://example.com/callback"
    )
    val login = auth.authorizationCode[IO](auth_client, credential)

    for {
      body <- login.login(protectedResource).use(_.expect[String](uri"/resource"))
      second_acquisition <- login.login(protectedResource).use_.attempt
      calls <- token_calls.get
    } yield {
      assertEquals(body, "ok")
      assertEquals(
        second_acquisition.left.map(_.getMessage),
        Left("authorization code login has already been acquired"))
      assertEquals(calls, 1)
    }
  }

  test("2f.authorizationCode keeps the code consumed after a failed exchange") {
    val token_calls = Ref.unsafe[IO, Int](0)
    val auth_client = Resource.pure[IO, Client[IO]](
      Client.fromHttpApp(HttpApp[IO] {
        case POST -> Root / "token" =>
          token_calls.update(_ + 1) *> InternalServerError("exchange failed")
        case _ => InternalServerError()
      })
    )
    val credential = AuthorizationCode(
      auth_endpoint = uri"/token",
      client_id = "client-id",
      client_secret = Secret("secret"),
      code = Secret("auth-code"),
      redirect_uri = "https://example.com/callback"
    )
    val login = auth.authorizationCode[IO](auth_client, credential)

    for {
      first_acquisition <- login.login(protectedResource).use_.attempt
      second_acquisition <- login.login(protectedResource).use_.attempt
      calls <- token_calls.get
    } yield {
      assert(first_acquisition.isLeft, "the failed token exchange should fail acquisition")
      assertEquals(
        second_acquisition.left.map(_.getMessage),
        Left("authorization code login has already been acquired"))
      assertEquals(calls, 1)
    }
  }

  test("2g.authorizationCode keeps the code consumed after a canceled exchange") {
    for {
      exchange_started <- Deferred[IO, Unit]
      token_calls <- Ref.of[IO, Int](0)
      auth_app = HttpApp[IO] {
        case POST -> Root / "token" =>
          token_calls.update(_ + 1) *>
            exchange_started.complete(()).void *>
            IO.never[Response[IO]]
        case _ => InternalServerError()
      }
      credential = AuthorizationCode(
        auth_endpoint = uri"/token",
        client_id = "client-id",
        client_secret = Secret("secret"),
        code = Secret("auth-code"),
        redirect_uri = "https://example.com/callback"
      )
      login = auth.authorizationCode[IO](Resource.pure(Client.fromHttpApp(auth_app)), credential)
      first_acquisition <- login.login(protectedResource).use_.start
      _ <- exchange_started.get
      _ <- first_acquisition.cancel
      second_acquisition <- login.login(protectedResource).use_.attempt
      calls <- token_calls.get
    } yield {
      assertEquals(
        second_acquisition.left.map(_.getMessage),
        Left("authorization code login has already been acquired"))
      assertEquals(calls, 1)
    }
  }

  test("2h.authorizationCode rejects an overlapping acquisition before allocating its auth client") {
    for {
      auth_allocations <- Ref.of[IO, Int](0)
      exchange_started <- Deferred[IO, Unit]
      allow_exchange <- Deferred[IO, Unit]
      auth_app = HttpApp[IO] {
        case POST -> Root / "token" =>
          exchange_started.complete(()).void *>
            allow_exchange.get *>
            Ok("""{"access_token":"token","token_type":"Bearer"}""")
        case _ => InternalServerError()
      }
      auth_client = Resource.make(
        auth_allocations.update(_ + 1).as(Client.fromHttpApp(auth_app))
      )(_ => IO.unit)
      credential = AuthorizationCode(
        auth_endpoint = uri"/token",
        client_id = "client-id",
        client_secret = Secret("secret"),
        code = Secret("auth-code"),
        redirect_uri = "https://example.com/callback"
      )
      login = auth.authorizationCode[IO](auth_client, credential)
      first_acquisition <- login.login(protectedResource).use_.start
      _ <- exchange_started.get
      second_acquisition <- login.login(protectedResource).use_.attempt
      _ <- allow_exchange.complete(())
      _ <- first_acquisition.joinWithNever
      allocations <- auth_allocations.get
    } yield {
      assertEquals(
        second_acquisition.left.map(_.getMessage),
        Left("authorization code login has already been acquired"))
      assertEquals(allocations, 1)
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

    login.login(protectedResource).use { authed =>
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

    auth.clientCredentials[IO](authClient, credential).login(resourceClient).use { authed =>
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

    auth.clientCredentials[IO](authClient, credential).login(resourceClient).use { authed =>
      authed.run(Request[IO](Method.GET, uri"/resource")).use { response =>
        IO(assertEquals(response.status, Status.Ok))
      }
    } *> unauthorizedReleases.get.map(releases => assertEquals(releases, 1))
  }

  test("4a1.unauthorized response releases a one-slot pool before retry") {
    val token_calls = Ref.unsafe[IO, Int](0)
    val auth_client = Resource.pure[IO, Client[IO]](
      Client.fromHttpApp(HttpApp[IO] {
        case POST -> Root / "token" =>
          token_calls.updateAndGet(_ + 1).flatMap { call =>
            val token = if (call == 1) "old-token" else "new-token"
            Ok(s"""{"access_token":"$token","token_type":"Bearer","expires_in":3600}""")
          }
        case _ => InternalServerError()
      })
    )

    cats.effect.std.Semaphore[IO](1).flatMap { pool_slot =>
      val resource_client = Client[IO] { request =>
        Resource.make(pool_slot.acquire)(_ => pool_slot.release).map { _ =>
          request.headers.get[Authorization] match {
            case Some(header) if header.value == "Bearer old-token" =>
              Response[IO](Status.Unauthorized)
            case Some(header) if header.value == "Bearer new-token" =>
              Response[IO](Status.Ok)
            case _ =>
              Response[IO](Status.Forbidden)
          }
        }
      }

      val credential = ClientCredentials(
        auth_endpoint = uri"/token",
        client_id = "id",
        client_secret = Secret("secret")
      )

      auth.clientCredentials[IO](auth_client, credential).login(resource_client).use { authed =>
        authed.run(Request[IO](Method.GET, uri"/resource")).use { response =>
          IO(assertEquals(response.status, Status.Ok))
        }
      }.timeout(1.second)
    }
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

    auth.clientCredentials[IO](authClient, credential).login(slowClient).use { authed =>
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

    auth.clientCredentials[IO](authClient, credential).login(alwaysUnauthorized).use { authed =>
      authed.run(Request[IO](Method.GET, uri"/resource")).use { response =>
        tokenCalls.get.map { n =>
          // Should see initial token fetch + one refresh on 401, then the second 401 is returned
          assertEquals(response.status, Status.Unauthorized)
          assertEquals(n, 2)
        }
      }
    }
  }

  test("4d.getTokenFromCredentials failure during 401 recovery propagates the error") {
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

    auth.clientCredentials[IO](authClient, credential).login(resourceClient).use { authed =>
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

  test("7.Salesforce password grant rewrites the URI and removes a stale Host header") {
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
          assertEquals(req.headers.get[Host].map(_.host), Some("example.my.salesforce.com"))
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

    Salesforce[IO](authClient, credential, 2.hours).login(Client.fromHttpApp(resourceApp)).use { authed =>
      val request = Request[IO](uri = uri"/resource").putHeaders(Host("original.example", None))
      authed.expect[String](request)
    }
  }

  test("8.Salesforce password grant preserves full path, query, and fragment after path-info translation") {
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
          assertEquals(req.uri.fragment, Some("details"))
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

    Salesforce[IO](authClient, credential, 2.hours).login(Client.fromHttpApp(resourceApp)).use { authed =>
      val request = Request[IO](
        uri = Uri.unsafeFromString("/services/data/v58.0/query?q=SELECT+Id+FROM+Account#details")
      ).withAttribute(Request.Keys.PathInfoCaret, 1)
      authed.expect[String](request)
    }
  }

  test("8a.Salesforce password grant: non-positive or absent expiresIn disables scheduled renewal") {
    val credential = Salesforce.PasswordGrant(
      auth_endpoint = uri"http://sf.test/token",
      client_id = "client-id",
      client_secret = Secret("secret"),
      username = "user",
      password = Secret("pass")
    )

    val token =
      """{"access_token":"sf-token","instance_url":"http://sf.test","id":"id","token_type":"bearer","issued_at":"0","signature":"sig"}"""

    // a Salesforce login whose token endpoint records how many times it is called; the business call is
    // routed to instance_url and simply succeeds.
    def loginWith(makeLogin: Resource[IO, Client[IO]] => Login[IO]): IO[Int] =
      cats.effect.testkit.TestControl.executeEmbed {
        Ref.of[IO, Int](0).flatMap { token_calls =>
          val auth_app = HttpApp[IO] {
            case POST -> Root / "token" => token_calls.update(_ + 1) *> Ok(token)
            case _                      => Ok("ok")
          }
          val auth_client = Resource.pure[IO, Client[IO]](Client.fromHttpApp(auth_app))
          makeLogin(auth_client)
            .login(Client.fromHttpApp(auth_app))
            .use(client => client.expect[String](uri"http://sf.test/resource") *> IO.sleep(10.hours))
            .flatMap(_ => token_calls.get)
        }
      }

    for {
      zero <- loginWith(Salesforce[IO](_, credential, Duration.Zero))
      negative <- loginWith(Salesforce[IO](_, credential, (-1).second))
      absent <- loginWith(Salesforce[IO](_, credential))
    } yield {
      // token is fetched once at startup; no scheduled renewal fires over 10 virtual hours
      assertEquals(zero, 1)
      assertEquals(negative, 1)
      assertEquals(absent, 1)
    }
  }

  test("8b.Salesforce password grant: positive expiresIn schedules a renewal before expiry") {
    cats.effect.testkit.TestControl.executeEmbed {
      val credential = Salesforce.PasswordGrant(
        auth_endpoint = uri"http://sf.test/token",
        client_id = "client-id",
        client_secret = Secret("secret"),
        username = "user",
        password = Secret("pass")
      )
      val token =
        """{"access_token":"sf-token","instance_url":"http://sf.test","id":"id","token_type":"bearer","issued_at":"0","signature":"sig"}"""

      Ref.of[IO, Int](0).flatMap { token_calls =>
        val auth_app = HttpApp[IO] {
          case POST -> Root / "token" => token_calls.update(_ + 1) *> Ok(token)
          case _                      => Ok("ok")
        }
        val auth_client = Resource.pure[IO, Client[IO]](Client.fromHttpApp(auth_app))

        Salesforce[IO](auth_client, credential, 100.seconds)
          .login(Client.fromHttpApp(auth_app))
          .use { _ =>
            // renewal is scheduled skewed early: 100s - min(30s, 50s) = 70s
            for {
              _ <- IO.sleep(69.seconds)
              before <- token_calls.get
              _ <- IO.sleep(2.seconds)
              after <- token_calls.get
            } yield {
              assertEquals(before, 1) // only the startup fetch so far
              assertEquals(after, 2) // scheduled renewal fired at ~70s
            }
          }
      }
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

    auth.clientCredentials[IO](authClient, credential).login(protectedResource).use { authed =>
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

    cats.effect.testkit.TestControl.executeEmbed {
      auth.clientCredentials[IO](authClient, credential).login(protectedResource).use { _ =>
        for {
          _ <- IO.sleep(499.millis)
          before_half_life <- tokenCalls.get
          _ <- IO.sleep(2.millis)
          after_half_life <- tokenCalls.get
        } yield {
          assertEquals(before_half_life, 1)
          assertEquals(after_half_life, 2)
        }
      }
    }
  }

  test("10.1.oauth renewal delay follows short and long lifetime boundaries") {
    def assert_renewal(lifetime_seconds: Long, expected_delay: FiniteDuration): IO[Unit] =
      cats.effect.testkit.TestControl.executeEmbed {
        val token_calls = Ref.unsafe[IO, Int](0)
        val auth_app = HttpApp[IO] { _ =>
          token_calls.updateAndGet(_ + 1).flatMap {
            case 1 =>
              Ok(s"""{"access_token":"token-1","token_type":"Bearer","expires_in":$lifetime_seconds}""")
            case _ =>
              Ok("""{"access_token":"token-2","token_type":"Bearer"}""")
          }
        }
        val credential = ClientCredentials(uri"/token", "id", Secret("secret"))

        auth.clientCredentials[IO](Resource.pure(Client.fromHttpApp(auth_app)), credential)
          .login(protectedResource)
          .use { _ =>
            for {
              _ <- IO.sleep(expected_delay - 1.millis)
              before <- token_calls.get
              _ <- IO.sleep(2.millis)
              after <- token_calls.get
            } yield {
              assertEquals(before, 1)
              assertEquals(after, 2)
            }
          }
      }

    def assert_extreme_lifetime_does_not_overflow: IO[Unit] =
      cats.effect.testkit.TestControl.executeEmbed {
        val token_calls = Ref.unsafe[IO, Int](0)
        val auth_app = HttpApp[IO] { _ =>
          token_calls.updateAndGet(_ + 1).flatMap { n =>
            Ok(s"""{"access_token":"token-$n","token_type":"Bearer","expires_in":${Long.MaxValue}}""")
          }
        }
        val credential = ClientCredentials(uri"/token", "id", Secret("secret"))

        auth.clientCredentials[IO](Resource.pure(Client.fromHttpApp(auth_app)), credential)
          .login(protectedResource)
          .use(_ => IO.sleep(1.second) *> token_calls.get.map(calls => assertEquals(calls, 1)))
      }

    List(
      1L -> 500.millis,
      60L -> 30.seconds,
      61L -> 31.seconds,
      3600L -> 3570.seconds
    ).traverse_ { case (lifetime_seconds, expected_delay) =>
      assert_renewal(lifetime_seconds, expected_delay)
    } *> assert_extreme_lifetime_does_not_overflow
  }

  test("10.2.non-positive expires_in accepts the token and disables scheduled renewal") {
    cats.effect.testkit.TestControl.executeEmbed {
      val client_credential = ClientCredentials(uri"/token", "id", Secret("secret"))
      val authorization_code = AuthorizationCode(
        auth_endpoint = uri"/token",
        client_id = "id",
        client_secret = Secret("secret"),
        code = Secret("code"),
        redirect_uri = "https://example.com/callback"
      )

      for {
        client_calls <- Ref.of[IO, Int](0)
        authorization_calls <- Ref.of[IO, Int](0)
        zero_client = Resource.pure[IO, Client[IO]](Client.fromHttpApp(HttpApp[IO] { _ =>
          client_calls.update(_ + 1) *>
            Ok("""{"access_token":"zero-token","token_type":"Bearer","expires_in":0}""")
        }))
        negative_client = Resource.pure[IO, Client[IO]](Client.fromHttpApp(HttpApp[IO] { _ =>
          authorization_calls.update(_ + 1) *>
            Ok("""{"access_token":"negative-token","refresh_token":"refresh","id_token":"id","token_type":"Bearer","expires_in":-1}""")
        }))
        client_result <- auth.clientCredentials[IO](zero_client, client_credential)
          .login(protectedResource)
          .use(client => client.expect[String](uri"/resource") <* IO.sleep(1.hour))
        authorization_result <- auth.authorizationCode[IO](negative_client, authorization_code)
          .login(protectedResource)
          .use(client => client.expect[String](uri"/resource") <* IO.sleep(1.hour))
        final_client_calls <- client_calls.get
        final_authorization_calls <- authorization_calls.get
      } yield {
        assertEquals(client_result, "ok")
        assertEquals(authorization_result, "ok")
        assertEquals(final_client_calls, 1)
        assertEquals(final_authorization_calls, 1)
      }
    }
  }

  test("10.3.non-positive expires_in replacements are published without another scheduled renewal") {
    val client_credential = ClientCredentials(uri"/token", "id", Secret("secret"))
    val authorization_code = AuthorizationCode(
      auth_endpoint = uri"/token",
      client_id = "id",
      client_secret = Secret("secret"),
      code = Secret("code"),
      redirect_uri = "https://example.com/callback"
    )

    for {
      client_token_calls <- Ref.of[IO, Int](0)
      client_business_tokens <- Ref.of[IO, List[String]](Nil)
      _ <- cats.effect.testkit.TestControl.executeEmbed {
        val auth_app = HttpApp[IO] { _ =>
          client_token_calls.updateAndGet(_ + 1).flatMap {
            case 1 =>
              Ok("""{"access_token":"old-token","token_type":"Bearer","expires_in":1,"refresh_token":"refresh"}""")
            case _ =>
              Ok("""{"access_token":"unscheduled-token","token_type":"Bearer","expires_in":0}""")
          }
        }
        val business_app = HttpApp[IO] { request =>
          val token = request.headers.get[Authorization].fold("missing")(_.value.stripPrefix("Bearer "))
          client_business_tokens.update(_ :+ token) *> Ok("ok")
        }

        auth.clientCredentials[IO](Resource.pure(Client.fromHttpApp(auth_app)), client_credential)
          .login(Client.fromHttpApp(business_app))
          .use { authed =>
            IO.sleep(501.millis) *> authed.expect[String](uri"/resource") *> IO.sleep(1.hour)
          }
      }
      client_calls <- client_token_calls.get
      published_client_tokens <- client_business_tokens.get
      authorization_token_calls <- Ref.of[IO, Int](0)
      authorization_business_tokens <- Ref.of[IO, List[String]](Nil)
      authorization_status <- cats.effect.testkit.TestControl.executeEmbed {
        val auth_app = HttpApp[IO] { request =>
          authorization_token_calls.update(_ + 1) *> request.as[UrlForm].flatMap { form =>
            form.getFirst("grant_type") match {
              case Some("authorization_code") =>
                Ok("""{"access_token":"old-token","refresh_token":"refresh","id_token":"id","token_type":"Bearer","expires_in":3600}""")
              case Some("refresh_token") =>
                Ok("""{"access_token":"unscheduled-token","refresh_token":"refresh-2","id_token":"id-2","token_type":"Bearer","expires_in":-1}""")
              case _ =>
                InternalServerError()
            }
          }
        }
        val business_app = HttpApp[IO] { request =>
          val token = request.headers.get[Authorization].fold("missing")(_.value.stripPrefix("Bearer "))
          authorization_business_tokens.update(_ :+ token) *>
            IO.pure(Response[IO](Status.Unauthorized))
        }

        auth.authorizationCode[IO](Resource.pure(Client.fromHttpApp(auth_app)), authorization_code)
          .login(Client.fromHttpApp(business_app))
          .use { authed =>
            authed.run(Request[IO](uri = uri"/resource")).use(response =>
              IO.sleep(1.hour).as(response.status))
          }
      }
      authorization_calls <- authorization_token_calls.get
      published_authorization_tokens <- authorization_business_tokens.get
    } yield {
      assertEquals(client_calls, 2)
      assertEquals(published_client_tokens, List("unscheduled-token"))
      assertEquals(authorization_calls, 2)
      assertEquals(authorization_status, Status.Unauthorized)
      assertEquals(published_authorization_tokens, List("old-token", "unscheduled-token"))
    }
  }

  test("10a.failed scheduled renewal waits for reactive replacement, then schedules the new token") {
    cats.effect.testkit.TestControl.executeEmbed {
      for {
        token_calls <- Ref.of[IO, Int](0)
        business_tokens <- Ref.of[IO, List[String]](Nil)
        auth_app = HttpApp[IO] {
          case POST -> Root / "token" =>
            token_calls.updateAndGet(_ + 1).flatMap {
              case 1 => Ok("""{"access_token":"t1","token_type":"Bearer","expires_in":130}""")
              case 2 => InternalServerError("scheduled renewal failed")
              case 3 => Ok("""{"access_token":"t2","token_type":"Bearer","expires_in":20}""")
              case 4 => Ok("""{"access_token":"t3","token_type":"Bearer","expires_in":0}""")
              case n => InternalServerError(s"unexpected token call $n")
            }
          case _ => InternalServerError()
        }
        business_app = HttpApp[IO] { request =>
          val token = request.headers.get[Authorization].fold("missing")(_.value.stripPrefix("Bearer "))
          business_tokens.update(_ :+ token) *>
            IO.pure(Response[IO](if (token == "t1") Status.Unauthorized else Status.Ok))
        }
        auth_client = Resource.pure[IO, Client[IO]](Client.fromHttpApp(auth_app))
        credential = ClientCredentials(uri"/token", "id", Secret("secret"))
        _ <- auth.clientCredentials[IO](auth_client, credential).login(Client.fromHttpApp(business_app)).use {
          authed =>
            for {
              // t1 renews at t=100. That scheduled attempt fails; no internal retry occurs before t=106.
              _ <- IO.sleep(106.seconds)
              after_failure <- token_calls.get
              status <- authed.status(Request[IO](uri = uri"/resource"))
              after_rejection <- token_calls.get
              // Reactive renewal installed t2 (20s lifetime), whose skewed delay is 10s from t=106.
              _ <- IO.sleep(9.seconds)
              before_resumed_schedule <- token_calls.get
              _ <- IO.sleep(2.seconds)
              after_resumed_schedule <- token_calls.get
              used_tokens <- business_tokens.get
            } yield {
              assertEquals(after_failure, 2) // startup + one failed scheduled attempt, no retry
              assertEquals(status, Status.Ok) // 401 on t1, then one successful replay with t2
              assertEquals(after_rejection, 3) // reactive replacement fetched t2
              assertEquals(before_resumed_schedule, 3)
              assertEquals(after_resumed_schedule, 4) // scheduling resumed and fetched t3
              assertEquals(used_tokens, List("t1", "t2"))
            }
        }
      } yield ()
    }
  }

  test("10b.scheduled and unauthorized refreshes share one successful replacement") {
    cats.effect.testkit.TestControl.executeEmbed {
      for {
        token_calls <- Ref.of[IO, Int](0)
        grant_types <- Ref.of[IO, List[String]](Nil)
        refresh_started <- Deferred[IO, Unit]
        allow_refresh <- Deferred[IO, Unit]
        stale_request_seen <- Deferred[IO, Unit]
        _ <- {
          val auth_app = HttpApp[IO] {
            case request @ POST -> Root / "token" =>
              request.as[UrlForm].flatMap { form =>
                val grant_type = form.getFirst("grant_type").getOrElse(fail("missing grant_type"))
                grant_types.update(_ :+ grant_type) *> token_calls.updateAndGet(_ + 1).flatMap {
                  case 1 =>
                    Ok("""{"access_token":"token-1","token_type":"Bearer","expires_in":1,"refresh_token":"refresh-1"}""")
                  case 2 =>
                    refresh_started.complete(()).flatMap(_ =>
                      allow_refresh.get *> IO.sleep(1.second) *>
                        Ok("""{"access_token":"token-2","token_type":"Bearer","expires_in":3600}"""))
                  case n =>
                    Ok(s"""{"access_token":"token-$n","token_type":"Bearer","expires_in":3600}""")
                }
              }
            case _ => InternalServerError()
          }

          val resource_app = HttpApp[IO] { request =>
            request.headers.get[Authorization] match {
              case Some(header) if header.value == "Bearer token-1" =>
                stale_request_seen.complete(()).flatMap(_ => IO.pure(Response[IO](Status.Unauthorized)))
              case Some(_) => Ok("ok")
              case None    => Forbidden("missing auth")
            }
          }

          val credential = ClientCredentials(
            auth_endpoint = uri"/token",
            client_id = "id",
            client_secret = Secret("secret")
          )

          auth
            .clientCredentials[IO](Resource.pure(Client.fromHttpApp(auth_app)), credential)
            .login(Client.fromHttpApp(resource_app))
            .use { authed =>
              for {
                _ <- IO.sleep(5.seconds)
                _ <- refresh_started.get
                request <- authed.expect[String](uri"/resource").start
                _ <- stale_request_seen.get
                _ <- allow_refresh.complete(())
                body <- request.joinWithNever
                calls <- token_calls.get
                grants <- grant_types.get
              } yield {
                assertEquals(body, "ok")
                assertEquals(calls, 2)
                assertEquals(grants, List("client_credentials", "refresh_token"))
              }
            }
        }
      } yield ()
    }
  }

  test("10c.failed scheduled refresh allows unauthorized recovery policy") {
    cats.effect.testkit.TestControl.executeEmbed {
      for {
        token_calls <- Ref.of[IO, Int](0)
        grant_types <- Ref.of[IO, List[String]](Nil)
        refresh_started <- Deferred[IO, Unit]
        allow_failure <- Deferred[IO, Unit]
        stale_request_seen <- Deferred[IO, Unit]
        _ <- {
          val auth_app = HttpApp[IO] {
            case request @ POST -> Root / "token" =>
              request.as[UrlForm].flatMap { form =>
                val grant_type = form.getFirst("grant_type").getOrElse(fail("missing grant_type"))
                grant_types.update(_ :+ grant_type) *> token_calls.updateAndGet(_ + 1).flatMap {
                  case 1 =>
                    Ok("""{"access_token":"token-1","token_type":"Bearer","expires_in":1,"refresh_token":"refresh-1"}""")
                  case 2 =>
                    refresh_started.complete(()).flatMap(_ =>
                      allow_failure.get *> InternalServerError("scheduled refresh failed"))
                  case n =>
                    Ok(s"""{"access_token":"token-$n","token_type":"Bearer","expires_in":3600}""")
                }
              }
            case _ => InternalServerError()
          }

          val resource_app = HttpApp[IO] { request =>
            request.headers.get[Authorization] match {
              case Some(header) if header.value == "Bearer token-1" =>
                stale_request_seen.complete(()).flatMap(_ => IO.pure(Response[IO](Status.Unauthorized)))
              case Some(_) => Ok("ok")
              case None    => Forbidden("missing auth")
            }
          }

          val credential = ClientCredentials(
            auth_endpoint = uri"/token",
            client_id = "id",
            client_secret = Secret("secret")
          )

          auth
            .clientCredentials[IO](Resource.pure(Client.fromHttpApp(auth_app)), credential)
            .login(Client.fromHttpApp(resource_app))
            .use { authed =>
              for {
                _ <- IO.sleep(5.seconds)
                _ <- refresh_started.get
                request <- authed.expect[String](uri"/resource").start
                _ <- stale_request_seen.get
                _ <- allow_failure.complete(())
                body <- request.joinWithNever
                calls <- token_calls.get
                grants <- grant_types.get
              } yield {
                assertEquals(body, "ok")
                assertEquals(calls, 3)
                assertEquals(grants, List("client_credentials", "refresh_token", "client_credentials"))
              }
            }
        }
      } yield ()
    }
  }

  test("10d.unauthorized refresh resets the scheduled renewal timer") {
    cats.effect.testkit.TestControl.executeEmbed {
      val token_calls = Ref.unsafe[IO, Int](0)
      val auth_app = HttpApp[IO] {
        case POST -> Root / "token" =>
          token_calls.updateAndGet(_ + 1).flatMap {
            case 1 =>
              Ok("""{"access_token":"token-1","token_type":"Bearer","expires_in":130}""")
            case 2 =>
              Ok("""{"access_token":"token-2","token_type":"Bearer","expires_in":40}""")
            case n =>
              Ok(s"""{"access_token":"token-$n","token_type":"Bearer","expires_in":3600}""")
          }
        case _ => InternalServerError()
      }

      val resource_app = HttpApp[IO] { request =>
        request.headers.get[Authorization] match {
          case Some(header) if header.value == "Bearer token-1" =>
            IO.pure(Response[IO](Status.Unauthorized))
          case Some(_) => Ok("ok")
          case None    => Forbidden("missing auth")
        }
      }

      val credential = ClientCredentials(
        auth_endpoint = uri"/token",
        client_id = "id",
        client_secret = Secret("secret")
      )

      auth
        .clientCredentials[IO](Resource.pure(Client.fromHttpApp(auth_app)), credential)
        .login(Client.fromHttpApp(resource_app))
        .use { authed =>
          for {
            _ <- IO.sleep(10.seconds)
            body <- authed.expect[String](uri"/resource")
            _ <- IO.sleep(19.seconds)
            before_new_schedule <- token_calls.get
            _ <- IO.sleep(2.seconds)
            after_new_schedule <- token_calls.get
          } yield {
            assertEquals(body, "ok")
            assertEquals(before_new_schedule, 2)
            assertEquals(after_new_schedule, 3)
          }
        }
    }
  }

  test("10e.unauthorized refresh starts scheduling after an unscheduled token") {
    cats.effect.testkit.TestControl.executeEmbed {
      val token_calls = Ref.unsafe[IO, Int](0)
      val auth_app = HttpApp[IO] {
        case POST -> Root / "token" =>
          token_calls.updateAndGet(_ + 1).flatMap {
            case 1 =>
              Ok("""{"access_token":"token-1","token_type":"Bearer"}""")
            case 2 =>
              Ok("""{"access_token":"token-2","token_type":"Bearer","expires_in":40}""")
            case n =>
              Ok(s"""{"access_token":"token-$n","token_type":"Bearer","expires_in":3600}""")
          }
        case _ => InternalServerError()
      }

      val resource_app = HttpApp[IO] { request =>
        request.headers.get[Authorization] match {
          case Some(header) if header.value == "Bearer token-1" =>
            IO.pure(Response[IO](Status.Unauthorized))
          case Some(_) => Ok("ok")
          case None    => Forbidden("missing auth")
        }
      }

      val credential = ClientCredentials(
        auth_endpoint = uri"/token",
        client_id = "id",
        client_secret = Secret("secret")
      )

      auth
        .clientCredentials[IO](Resource.pure(Client.fromHttpApp(auth_app)), credential)
        .login(Client.fromHttpApp(resource_app))
        .use { authed =>
          for {
            body <- authed.expect[String](uri"/resource")
            _ <- IO.sleep(19.seconds)
            before_schedule <- token_calls.get
            _ <- IO.sleep(2.seconds)
            after_schedule <- token_calls.get
          } yield {
            assertEquals(body, "ok")
            assertEquals(before_schedule, 2)
            assertEquals(after_schedule, 3)
          }
        }
    }
  }

  test("10f.releasing the authenticated client cancels a blocked renewal") {
    cats.effect.testkit.TestControl.executeEmbed {
      for {
        token_calls <- Ref.of[IO, Int](0)
        renewal_started <- Deferred[IO, Unit]
        renewal_canceled <- Deferred[IO, Unit]
        _ <- {
          val auth_app = HttpApp[IO] {
            case POST -> Root / "token" =>
              token_calls.updateAndGet(_ + 1).flatMap {
                case 1 =>
                  Ok("""{"access_token":"token-1","token_type":"Bearer","expires_in":1}""")
                case _ =>
                  renewal_started.complete(()).void *>
                    IO.never[Response[IO]].onCancel(renewal_canceled.complete(()).void)
              }
            case _ => InternalServerError()
          }

          val credential = ClientCredentials(
            auth_endpoint = uri"/token",
            client_id = "id",
            client_secret = Secret("secret")
          )

          auth
            .clientCredentials[IO](Resource.pure(Client.fromHttpApp(auth_app)), credential)
            .login(protectedResource)
            .use(_ => IO.sleep(5.seconds) *> renewal_started.get)
        }
        _ <- renewal_canceled.get
        _ <- IO.sleep(100.seconds)
        calls <- token_calls.get
      } yield assertEquals(calls, 2)
    }
  }

  test("11.concurrent 401s share one token refresh") {
    for {
      token_calls <- Ref.of[IO, Int](0)
      stale_requests <- Ref.of[IO, Int](0)
      all_stale_requests_seen <- Deferred[IO, Unit]
      release_stale_responses <- Deferred[IO, Unit]
      _ <- {
        val auth_app = HttpApp[IO] {
          case POST -> Root / "token" =>
            token_calls.updateAndGet(_ + 1).flatMap { n =>
              Ok(s"""{"access_token":"token-$n","token_type":"Bearer","expires_in":3600}""")
            }
          case _ => InternalServerError()
        }

        val resource_app = HttpApp[IO] { request =>
          request.headers.get[Authorization] match {
            case Some(header) if header.value == "Bearer token-1" =>
              stale_requests.updateAndGet(_ + 1).flatMap { count =>
                val signal = if (count == 5) all_stale_requests_seen.complete(()) else IO.pure(false)
                signal *> release_stale_responses.get *> IO.pure(Response[IO](Status.Unauthorized))
              }
            case Some(_) => Ok("ok")
            case None    => Forbidden("missing auth")
          }
        }

        val credential = ClientCredentials(
          auth_endpoint = uri"/token",
          client_id = "id",
          client_secret = Secret("secret")
        )

        auth
          .clientCredentials[IO](Resource.pure(Client.fromHttpApp(auth_app)), credential)
          .login(Client.fromHttpApp(resource_app))
          .use { authed =>
            for {
              batch <- List.fill(5)(authed.expect[String](uri"/data")).parSequence.start
              _ <- all_stale_requests_seen.get
              _ <- release_stale_responses.complete(())
              results <- batch.joinWithNever
              calls <- token_calls.get
            } yield {
              results.foreach(result => assertEquals(result, "ok"))
              assertEquals(calls, 2)
            }
          }
      }
    } yield ()
  }

  test("11a.stale 401 retries the current token without refreshing again") {
    for {
      token_calls <- Ref.of[IO, Int](0)
      delayed_request_seen <- Deferred[IO, Unit]
      first_refresh_completed <- Deferred[IO, Unit]
      _ <- {
        val auth_app = HttpApp[IO] {
          case POST -> Root / "token" =>
            token_calls.updateAndGet(_ + 1).flatMap { n =>
              Ok(s"""{"access_token":"token-$n","token_type":"Bearer","expires_in":3600}""")
            }
          case _ => InternalServerError()
        }

        val resource_app = HttpApp[IO] { request =>
          val token = request.headers.get[Authorization].map(_.value.stripPrefix("Bearer "))
          (token, request.uri.path.renderString) match {
            case (Some("token-1"), "/delayed") =>
              delayed_request_seen.complete(()).flatMap(_ =>
                first_refresh_completed.get *> IO.pure(Response[IO](Status.Unauthorized)))
            case (Some("token-1"), "/first") =>
              IO.pure(Response[IO](Status.Unauthorized))
            case (Some("token-2"), "/first") =>
              first_refresh_completed.complete(()).flatMap(_ => Ok("ok"))
            case (Some(token), _) if token != "token-1" =>
              Ok("ok")
            case _ =>
              Forbidden("missing auth")
          }
        }

        val credential = ClientCredentials(
          auth_endpoint = uri"/token",
          client_id = "id",
          client_secret = Secret("secret")
        )

        auth
          .clientCredentials[IO](Resource.pure(Client.fromHttpApp(auth_app)), credential)
          .login(Client.fromHttpApp(resource_app))
          .use { authed =>
            for {
              delayed <- authed.expect[String](uri"/delayed").start
              _ <- delayed_request_seen.get
              first <- authed.expect[String](uri"/first")
              second <- delayed.joinWithNever
              calls <- token_calls.get
            } yield {
              assertEquals(first, "ok")
              assertEquals(second, "ok")
              assertEquals(calls, 2)
            }
          }
      }
    } yield ()
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

    auth.clientCredentials[IO](authClient, credential).login(clientResource).use { authed =>
      authed.expect[String](uri"/hello").map(body => assertEquals(body, "ok"))
    }
  }
}
