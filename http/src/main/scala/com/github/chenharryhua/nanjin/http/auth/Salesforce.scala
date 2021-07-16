package com.github.chenharryhua.nanjin.http.auth

import cats.effect.Async
import cats.effect.kernel.Resource
import fs2.Stream
import io.circe.generic.auto.*
import org.http4s.*
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.client.middleware.Retry
import org.http4s.implicits.http4sLiteralsSyntax

import scala.concurrent.duration.*

sealed abstract class SalesforceToken(val name: String)

object SalesforceToken {
  final private case class IotToken(
    access_token: String,
    instance_url: String,
    id: String,
    token_type: String,
    issued_at: String,
    signature: String)

  final private case class McToken(
    access_token: String,
    token_type: String,
    expires_in: Long, // in seconds
    scope: String,
    soap_instance_url: String,
    rest_instance_url: String)

  sealed private trait InstanceURL
  private case object Rest extends InstanceURL
  private case object Soap extends InstanceURL

  //https://developer.salesforce.com/docs/atlas.en-us.mc-app-development.meta/mc-app-development/authorization-code.htm
  final class MarketingCloud[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String,
    instanceURL: InstanceURL
  ) extends SalesforceToken("salesforce_mc") with Http4sClientDsl[F] with Login[F] {

    override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {
      val getToken: Stream[F, McToken] =
        Stream.eval(
          Retry(authPolicy[F])(client).expect[McToken](
            POST(
              UrlForm(
                "grant_type" -> "client_credentials",
                "client_id" -> client_id,
                "client_secret" -> client_secret
              ),
              auth_endpoint.withPath(path"/v2/token")
            )))

      getToken.evalMap(F.ref).flatMap { token =>
        val refresh: Stream[F, Unit] =
          Stream.eval(token.get).flatMap(t => getToken.delayBy(t.expires_in.seconds).evalMap(token.set)).repeat
        Stream[F, Client[F]](Client[F] { req =>
          Resource.eval(token.get).flatMap { t =>
            val iu: Uri = instanceURL match {
              case Rest => Uri.unsafeFromString(t.rest_instance_url).withPath(req.pathInfo)
              case Soap => Uri.unsafeFromString(t.soap_instance_url).withPath(req.pathInfo)
            }
            client.run(req.withUri(iu).putHeaders(Headers("Authorization" -> s"${t.token_type} ${t.access_token}")))
          }
        }).concurrently(refresh)
      }
    }
  }
  object MarketingCloud {
    def rest[F[_]](auth_endpoint: Uri, client_id: String, client_secret: String): MarketingCloud[F] =
      new MarketingCloud[F](auth_endpoint, client_id, client_secret, Rest)
    def soap[F[_]](auth_endpoint: Uri, client_id: String, client_secret: String): MarketingCloud[F] =
      new MarketingCloud[F](auth_endpoint, client_id, client_secret, Soap)
  }

  //https://developer.salesforce.com/docs/atlas.en-us.api_iot.meta/api_iot/qs_auth_access_token.htm
  final class Iot[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String,
    username: String,
    password: String
  ) extends SalesforceToken("salesforce_iot") with Http4sClientDsl[F] with Login[F] {

    override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {
      val getToken: Stream[F, IotToken] =
        Stream.eval(
          Retry(authPolicy[F])(client).expect[IotToken](POST(
            UrlForm(
              "grant_type" -> "password",
              "client_id" -> client_id,
              "client_secret" -> client_secret,
              "username" -> username,
              "password" -> password
            ),
            auth_endpoint.withPath(path"/services/oauth2/token")
          )))

      getToken.evalMap(F.ref).flatMap { token =>
        val refresh: Stream[F, Unit] = getToken.delayBy(1.hour).evalMap(token.set).repeat
        Stream[F, Client[F]](Client[F] { req =>
          Resource
            .eval(token.get)
            .flatMap(t =>
              client.run(
                req
                  .withUri(Uri.unsafeFromString(t.instance_url).withPath(req.pathInfo))
                  .putHeaders(Headers("Authorization" -> s"${t.token_type} ${t.access_token}"))))
        }).concurrently(refresh)
      }
    }
  }
  object Iot {
    def apply[F[_]](
      auth_endpoint: Uri,
      client_id: String,
      client_secret: String,
      username: String,
      password: String): Iot[F] =
      new Iot[F](auth_endpoint, client_id, client_secret, username, password)
  }
}
