package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.implicits.genTemporalOps_
import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.all.*
import cats.Endo
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

import scala.concurrent.duration.{DurationLong, FiniteDuration}

object salesforce {

  // ??? https://developer.salesforce.com/docs/atlas.en-us.mc-app-development.meta/mc-app-development/authorization-code.htm

  sealed private trait InstanceURL
  private case object Rest extends InstanceURL
  private case object Soap extends InstanceURL

  final class MarketingCloud[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String,
    instanceURL: InstanceURL,
    cfg: AuthConfig,
    middleware: Endo[Client[F]]
  ) extends Http4sClientDsl[F] with Login[F, MarketingCloud[F]]
      with UpdateConfig[AuthConfig, MarketingCloud[F]] {

    private case class Token(
      access_token: String,
      token_type: String,
      expires_in: Long, // in seconds
      scope: String,
      soap_instance_url: String,
      rest_instance_url: String)

    private val params: AuthParams = cfg.evalConfig

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
            ))

      def updateToken(ref: Ref[F, Token]): F[Unit] =
        for {
          oldToken <- ref.get
          newToken <- getToken.delayBy(params.dormant(oldToken.expires_in.seconds))
          _ <- ref.set(newToken)
        } yield ()

      def withToken(token: Token, req: Request[F]): Request[F] = {
        val iu: Uri = instanceURL match {
          case Rest => Uri.unsafeFromString(token.rest_instance_url).withPath(req.pathInfo)
          case Soap => Uri.unsafeFromString(token.soap_instance_url).withPath(req.pathInfo)
        }
        req
          .withUri(iu)
          .putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))
      }

      loginInternal(middleware(client), getToken, updateToken, withToken)

    }

    override def updateConfig(f: Endo[AuthConfig]): MarketingCloud[F] =
      new MarketingCloud[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        instanceURL = instanceURL,
        cfg = f(cfg),
        middleware = middleware)

    override def withMiddleware(f: Endo[Client[F]]): MarketingCloud[F] =
      new MarketingCloud[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        instanceURL = instanceURL,
        cfg = cfg,
        middleware = middleware.compose(f))
  }

  object MarketingCloud {
    def rest[F[_]](auth_endpoint: Uri, client_id: String, client_secret: String): MarketingCloud[F] =
      new MarketingCloud[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        instanceURL = Rest,
        cfg = AuthConfig(),
        middleware = identity
      )
    def soap[F[_]](auth_endpoint: Uri, client_id: String, client_secret: String): MarketingCloud[F] =
      new MarketingCloud[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        instanceURL = Soap,
        cfg = AuthConfig(),
        middleware = identity
      )
  }

  // https://developer.salesforce.com/docs/atlas.en-us.api_iot.meta/api_iot/qs_auth_access_token.htm
  final class Iot[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String,
    username: String,
    password: String,
    expiresIn: FiniteDuration,
    cfg: AuthConfig,
    middleware: Endo[Client[F]]
  ) extends Http4sClientDsl[F] with Login[F, Iot[F]] with UpdateConfig[AuthConfig, Iot[F]] {

    private case class Token(
      access_token: String,
      instance_url: String,
      id: String,
      token_type: String,
      issued_at: String,
      signature: String)

    private val params: AuthParams = cfg.evalConfig

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
          ))

      def updateToken(ref: Ref[F, Token]): F[Unit] =
        getToken.delayBy(params.dormant(expiresIn)).flatMap(ref.set)

      def withToken(token: Token, req: Request[F]): Request[F] =
        req
          .withUri(Uri.unsafeFromString(token.instance_url).withPath(req.pathInfo))
          .putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))

      loginInternal(middleware(client), getToken, updateToken, withToken)
    }

    override def updateConfig(f: Endo[AuthConfig]): Iot[F] =
      new Iot[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        username = username,
        password = password,
        expiresIn = expiresIn,
        cfg = f(cfg),
        middleware = middleware
      )

    override def withMiddleware(f: Endo[Client[F]]): Iot[F] =
      new Iot[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        username = username,
        password = password,
        expiresIn = expiresIn,
        cfg = cfg,
        middleware = middleware.compose(f)
      )
  }

  object Iot {
    def apply[F[_]](
      auth_endpoint: Uri,
      client_id: String,
      client_secret: String,
      username: String,
      password: String,
      expiresIn: FiniteDuration): Iot[F] =
      new Iot[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        username = username,
        password = password,
        expiresIn = expiresIn,
        cfg = AuthConfig(),
        middleware = identity
      )
  }
}
