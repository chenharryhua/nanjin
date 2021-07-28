package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.Kleisli
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.{Applicative, Monad}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import fs2.Stream
import io.circe.generic.auto.*
import org.http4s.*
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.ci.CIString

import scala.concurrent.duration.DurationLong

object salesforce {

  //https://developer.salesforce.com/docs/atlas.en-us.mc-app-development.meta/mc-app-development/authorization-code.htm
  final private case class McToken(
    access_token: String,
    token_type: String,
    expires_in: Long, // in seconds
    scope: String,
    soap_instance_url: String,
    rest_instance_url: String)

  implicit private val expiralbeMcToken: IsExpirableToken[McToken] = (a: McToken) => a.expires_in.seconds

  sealed private trait InstanceURL
  private case object Rest extends InstanceURL
  private case object Soap extends InstanceURL

  final class MarketingCloud[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String,
    instanceURL: InstanceURL,
    config: AuthConfig,
    middleware: Kleisli[F, Client[F], Client[F]]
  ) extends Http4sClientDsl[F] with Login[F, MarketingCloud[F]] with UpdateConfig[AuthConfig, MarketingCloud[F]] {

    val params: AuthParams = config.evalConfig

    override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {
      val getToken: Stream[F, McToken] =
        Stream.eval(
          params
            .authClient(client)
            .expect[McToken](
              POST(
                UrlForm(
                  "grant_type" -> "client_credentials",
                  "client_id" -> client_id,
                  "client_secret" -> client_secret
                ),
                auth_endpoint.withPath(path"/v2/token")
              ).putHeaders("Cache-Control" -> "no-cache")))

      getToken.evalMap(F.ref).flatMap { token =>
        val refresh: Stream[F, Unit] =
          Stream.eval(token.get).flatMap(t => getToken.delayBy(params.calcDelay(t)).evalMap(token.set)).repeat

        Stream
          .eval(middleware(client))
          .map { client =>
            Client[F] { req =>
              Resource.eval(token.get).flatMap { t =>
                val iu: Uri = instanceURL match {
                  case Rest => Uri.unsafeFromString(t.rest_instance_url).withPath(req.pathInfo)
                  case Soap => Uri.unsafeFromString(t.soap_instance_url).withPath(req.pathInfo)
                }
                client.run(
                  req.withUri(iu).putHeaders(Authorization(Credentials.Token(CIString(t.token_type), t.access_token))))
              }
            }
          }
          .concurrently(refresh)
      }
    }

    override def updateConfig(f: AuthConfig => AuthConfig): MarketingCloud[F] =
      new MarketingCloud[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        instanceURL = instanceURL,
        config = f(config),
        middleware = middleware)

    override def withMiddlewareM(f: Client[F] => F[Client[F]])(implicit F: Monad[F]): MarketingCloud[F] =
      new MarketingCloud[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        instanceURL = instanceURL,
        config = config,
        middleware = compose(f, middleware))
  }

  object MarketingCloud {
    def rest[F[_]](auth_endpoint: Uri, client_id: String, client_secret: String)(implicit
      F: Applicative[F]): MarketingCloud[F] =
      new MarketingCloud[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        instanceURL = Rest,
        config = AuthConfig(2.hour),
        middleware = Kleisli(F.pure))
    def soap[F[_]](auth_endpoint: Uri, client_id: String, client_secret: String)(implicit
      F: Applicative[F]): MarketingCloud[F] =
      new MarketingCloud[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        instanceURL = Soap,
        config = AuthConfig(2.hour),
        middleware = Kleisli(F.pure))
  }

  //https://developer.salesforce.com/docs/atlas.en-us.api_iot.meta/api_iot/qs_auth_access_token.htm

  final private case class IotToken(
    access_token: String,
    instance_url: String,
    id: String,
    token_type: String,
    issued_at: String,
    signature: String)

  final class Iot[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String,
    username: String,
    password: String,
    config: AuthConfig,
    middleware: Kleisli[F, Client[F], Client[F]]
  ) extends Http4sClientDsl[F] with Login[F, Iot[F]] with UpdateConfig[AuthConfig, Iot[F]] {

    val params: AuthParams = config.evalConfig

    override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {
      val getToken: Stream[F, IotToken] =
        Stream.eval(
          params
            .authClient(client)
            .expect[IotToken](POST(
              UrlForm(
                "grant_type" -> "password",
                "client_id" -> client_id,
                "client_secret" -> client_secret,
                "username" -> username,
                "password" -> password
              ),
              auth_endpoint.withPath(path"/services/oauth2/token")
            ).putHeaders("Cache-Control" -> "no-cache")))

      getToken.evalMap(F.ref).flatMap { token =>
        val refresh: Stream[F, Unit] = getToken.delayBy(params.calcDelay).evalMap(token.set).repeat

        Stream
          .eval(middleware(client))
          .map { client =>
            Client[F] { req =>
              Resource
                .eval(token.get)
                .flatMap(t =>
                  client.run(req
                    .withUri(Uri.unsafeFromString(t.instance_url).withPath(req.pathInfo))
                    .putHeaders(Authorization(Credentials.Token(CIString(t.token_type), t.access_token)))))
            }
          }
          .concurrently(refresh)
      }
    }

    override def updateConfig(f: AuthConfig => AuthConfig): Iot[F] =
      new Iot[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        username = username,
        password = password,
        config = f(config),
        middleware = middleware)

    def withMiddlewareM(f: Client[F] => F[Client[F]])(implicit F: Monad[F]): Iot[F] =
      new Iot[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        username = username,
        password = password,
        config = config,
        middleware = compose(f, middleware))
  }

  object Iot {
    def apply[F[_]](auth_endpoint: Uri, client_id: String, client_secret: String, username: String, password: String)(
      implicit F: Applicative[F]): Iot[F] =
      new Iot[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        username = username,
        password = password,
        config = AuthConfig(2.hours),
        middleware = Kleisli(F.pure))
  }
}
