package com.github.chenharryhua.nanjin.http.salesforce.client

import cats.effect.{Async, Ref, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.http.salesforce.auth.{CredLoginRequest, TokenHasInstanceUrl, TokenProperties}
import fs2.Stream
import io.circe.generic.auto.*
import io.circe.syntax.*
import io.circe.{Decoder, Encoder}
import org.http4s.*
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.circe.CirceEntityEncoder.*
import org.http4s.circe.jsonOf
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.io.{PATCH, POST}
import retry.RetryPolicies

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

sealed trait SalesforceClient[F[_], Token] {
  def getToken: F[Token]
  def refreshToken: Stream[F, Unit]

  def post[A: Encoder](uri: Uri, payload: A): F[Either[SalesforceException, SalesforceResponse]]

  def post[A: Encoder](path: HttpPath, payload: A)(implicit
    ev: TokenHasInstanceUrl[F, Token]): F[Either[SalesforceException, SalesforceResponse]]

  def patch[A: Encoder](path: HttpPath, payload: A)(implicit
    ev: TokenHasInstanceUrl[F, Token]): F[Either[SalesforceException, SalesforceResponse]]
}

object SalesforceClient {

  def apply[F[_]: Async, Cred, Token: Decoder](cred: Cred)(implicit
    ec: ExecutionContext,
    login: CredLoginRequest[F, Cred],
    tokenProps: TokenProperties[Token]): Stream[F, SalesforceClient[F, Token]] =
    for {
      client <- BlazeClientBuilder[F](ec).withResponseHeaderTimeout(30.second).stream
      token <- Stream.eval(client.expect(login.loginRequest(cred))(jsonOf[F, Token]).flatMap(Ref.of[F, Token]))
    } yield new SalesforceClientImpl[F, Cred, Token](cred, client, token, login, tokenProps)

  final private class SalesforceClientImpl[F[_]: Async, Cred, Token](
    cred: Cred,
    client: Client[F],
    token: Ref[F, Token],
    login: CredLoginRequest[F, Cred],
    tokenProps: TokenProperties[Token])(implicit ev: Decoder[Token])
      extends SalesforceClient[F, Token] with Http4sClientDsl[F] {

    override def getToken: F[Token] = token.get

    override val refreshToken: Stream[F, Unit] =
      Stream.eval(token.get).flatMap { pt =>
        Stream.awakeEvery[F](tokenProps.ttl(pt)).evalMap { _ =>
          retry
            .retryingOnAllErrors[Token](
              RetryPolicies.limitRetries[F](10).join(RetryPolicies.constantDelay[F](5.seconds)),
              retry.noop[F, Throwable])(client.expect(login.loginRequest(cred))(jsonOf[F, Token]))
            .flatMap(token.set)
        }
      }

    private def buildResponse(request: Request[F], response: Response[F]): F[SalesforceResponse] =
      for {
        req <- request.bodyText.compile.toList
        resp <- response.bodyText.compile.toList
      } yield SalesforceResponse(
        httpVersion = request.httpVersion,
        method = request.method,
        uri = request.uri,
        reqeustHeaders = request.headers,
        requestBody = req.mkString,
        status = response.status,
        responseHeaders = response.headers,
        responseBody = resp.mkString
      )

    private def postReq[A: Encoder](uri: Uri, payload: A, authStr: String): Request[F] =
      POST(payload.asJson, uri).withHeaders(Headers(("Authorization", authStr), ("Content-Type", "application/json")))

    private def patchReq[A: Encoder](uri: Uri, payload: A, authStr: String): Request[F] =
      PATCH(payload.asJson, uri).withHeaders(Headers(("Authorization", authStr), ("Content-Type", "application/json")))

    private def runRequest(req: Request[F]): F[Either[SalesforceException, SalesforceResponse]] =
      client.run(req).use {
        case s if s.status.isSuccess =>
          buildResponse(req, s).map(Right(_))
        case Status.ServerError(ex) =>
          buildResponse(req, ex).map { r =>
            Left(SalesforceException(r, "Server side error"))
          }
        case Status.Unauthorized(ex) =>
          buildResponse(req, ex).flatMap { r =>
            Sync[F].raiseError(SalesforceException(r, "Authentication error"))
          }
        case Status.ClientError(ex) =>
          buildResponse(req, ex).map { r =>
            Left(SalesforceException(r, "Client side error"))
          }
        case ex =>
          buildResponse(req, ex).map { r =>
            Left(SalesforceException(r, "Unclassifed error"))
          }
      }

    override def patch[P: Encoder](path: HttpPath, payload: P)(implicit
      ev: TokenHasInstanceUrl[F, Token]): F[Either[SalesforceException, SalesforceResponse]] =
      for {
        ctoken <- token.get
        uri <- ev.instanceUrl(ctoken).map(_.withPath(Uri.Path.unsafeFromString(path.value)))
        req = patchReq(uri, payload, tokenProps.authStr(ctoken))
        rst <- runRequest(req)
      } yield rst

    override def post[A: Encoder](uri: Uri, payload: A): F[Either[SalesforceException, SalesforceResponse]] =
      for {
        ctoken <- token.get
        req = postReq(uri, payload, tokenProps.authStr(ctoken))
        rst <- runRequest(req)
      } yield rst

    override def post[A: Encoder](path: HttpPath, payload: A)(implicit
      ev: TokenHasInstanceUrl[F, Token]): F[Either[SalesforceException, SalesforceResponse]] =
      for {
        ctoken <- token.get
        uri <- ev.instanceUrl(ctoken).map(_.withPath(Uri.Path.unsafeFromString(path.value)))
        req = postReq(uri, payload, tokenProps.authStr(ctoken))
        rst <- runRequest(req)
      } yield rst
  }
}
