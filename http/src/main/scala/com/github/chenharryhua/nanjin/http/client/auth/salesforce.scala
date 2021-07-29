package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.Reader
import cats.effect.implicits.genTemporalOps_
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Supervisor
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.UpdateConfig
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

  sealed private trait InstanceURL
  private case object Rest extends InstanceURL
  private case object Soap extends InstanceURL

  final class MarketingCloud[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String,
    instanceURL: InstanceURL,
    config: AuthConfig,
    middleware: Reader[Client[F], Resource[F, Client[F]]]
  ) extends Http4sClientDsl[F] with Login[F, MarketingCloud[F]] with UpdateConfig[AuthConfig, MarketingCloud[F]] {
    private case class Token(
      access_token: String,
      token_type: String,
      expires_in: Long, // in seconds
      scope: String,
      soap_instance_url: String,
      rest_instance_url: String)

    implicit private val expirable: IsExpirableToken[Token] = (a: Token) => a.expires_in.seconds

    val params: AuthParams = config.evalConfig

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] = {
      val getToken: F[Token] =
        params
          .authClient(client)
          .expect[Token](
            POST(
              UrlForm(
                "grant_type" -> "client_credentials",
                "client_id" -> client_id,
                "client_secret" -> client_secret
              ),
              auth_endpoint.withPath(path"/v2/token")
            ).putHeaders("Cache-Control" -> "no-cache"))

      def updateToken(ref: Ref[F, Either[Throwable, Token]]): F[Unit] = for {
        newToken <- ref.get.flatMap {
          case Left(_)      => getToken.delayBy(params.whenNext).attempt
          case Right(value) => getToken.delayBy(params.whenNext(value)).attempt
        }
        _ <- ref.set(newToken)
      } yield ()

      for {
        supervisor <- Supervisor[F]
        ref <- Resource.eval(getToken.attempt.flatMap(F.ref))
        _ <- Resource.eval(supervisor.supervise(updateToken(ref).foreverM))
        c <- middleware(client)
      } yield Client[F] { req =>
        for {
          token <- Resource.eval(ref.get.rethrow)
          iu: Uri = instanceURL match {
            case Rest => Uri.unsafeFromString(token.rest_instance_url).withPath(req.pathInfo)
            case Soap => Uri.unsafeFromString(token.soap_instance_url).withPath(req.pathInfo)
          }
          out <- c.run(
            req
              .withUri(iu)
              .putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token))))
        } yield out
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

    override def withMiddlewareR(f: Client[F] => Resource[F, Client[F]]): MarketingCloud[F] =
      new MarketingCloud[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        instanceURL = instanceURL,
        config = config,
        middleware = compose(f, middleware))
  }

  object MarketingCloud {
    def rest[F[_]](auth_endpoint: Uri, client_id: String, client_secret: String): MarketingCloud[F] =
      new MarketingCloud[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        instanceURL = Rest,
        config = AuthConfig(2.hour),
        middleware = Reader(Resource.pure))
    def soap[F[_]](auth_endpoint: Uri, client_id: String, client_secret: String): MarketingCloud[F] =
      new MarketingCloud[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        instanceURL = Soap,
        config = AuthConfig(2.hour),
        middleware = Reader(Resource.pure))
  }

  //https://developer.salesforce.com/docs/atlas.en-us.api_iot.meta/api_iot/qs_auth_access_token.htm
  final class Iot[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String,
    username: String,
    password: String,
    config: AuthConfig,
    middleware: Reader[Client[F], Resource[F, Client[F]]]
  ) extends Http4sClientDsl[F] with Login[F, Iot[F]] with UpdateConfig[AuthConfig, Iot[F]] {
    private case class Token(
      access_token: String,
      instance_url: String,
      id: String,
      token_type: String,
      issued_at: String,
      signature: String)

    val params: AuthParams = config.evalConfig

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] = {
      val getToken: F[Token] =
        params
          .authClient(client)
          .expect[Token](POST(
            UrlForm(
              "grant_type" -> "password",
              "client_id" -> client_id,
              "client_secret" -> client_secret,
              "username" -> username,
              "password" -> password
            ),
            auth_endpoint.withPath(path"/services/oauth2/token")
          ).putHeaders("Cache-Control" -> "no-cache"))

      def updateToken(ref: Ref[F, Either[Throwable, Token]]): F[Unit] = for {
        newToken <- getToken.delayBy(params.whenNext).attempt
        _ <- ref.set(newToken)
      } yield ()

      for {
        supervisor <- Supervisor[F]
        ref <- Resource.eval(getToken.attempt.flatMap(F.ref))
        _ <- Resource.eval(supervisor.supervise(updateToken(ref).foreverM))
        c <- middleware(client)
      } yield Client[F] { req =>
        for {
          token <- Resource.eval(ref.get.rethrow)
          out <- c.run(
            req
              .withUri(Uri.unsafeFromString(token.instance_url).withPath(req.pathInfo))
              .putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token))))
        } yield out
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

    override def withMiddlewareR(f: Client[F] => Resource[F, Client[F]]): Iot[F] =
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
    def apply[F[_]](
      auth_endpoint: Uri,
      client_id: String,
      client_secret: String,
      username: String,
      password: String): Iot[F] =
      new Iot[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        username = username,
        password = password,
        config = AuthConfig(2.hours),
        middleware = Reader(Resource.pure))
  }
}
