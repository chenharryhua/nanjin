package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.applicativeError.given
import cats.syntax.eq.given
import cats.syntax.flatMap.given
import cats.syntax.show.showInterpolator
import com.github.chenharryhua.nanjin.common.resilience.SingleFlight
import org.http4s.Method.POST
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.`Idempotency-Key`
import org.http4s.{EntityDecoder, Request, Response, Status, Uri, UrlForm}

import java.util.UUID

/** Wraps an HTTP client with authentication. */
trait Login[F[_]] {

  def login(client: Client[F]): Resource[F, Client[F]]

  final def login(client: Resource[F, Client[F]]): Resource[F, Client[F]] =
    client.flatMap(login)

}

/** Provides token-based authentication for an HTTP client.
  *
  * Manages fetching, refreshing, and applying tokens to requests.
  *
  * Subclasses need to implement:
  *   - `getToken`: how to obtain a token without using a current token
  *   - `refreshToken`: how to replace a token rejected by the protected resource
  *   - `renewToken`: how to refresh or schedule token renewal
  *   - `withToken`: how to attach the token to an HTTP request
  */
abstract private class TokenAuthClient[F[_]](using F: Async[F]) extends Http4sClientDsl[F] {
  protected type T // token type
  protected def getToken: F[T]
  protected def refreshToken: T => F[T]
  protected def renewToken(ref: Ref[F, T]): F[Unit]
  protected def withToken(token: T, req: Request[F]): Request[F]

  final protected def postToken[A: EntityDecoder[F, *]](
    client: Client[F],
    auth_endpoint: Uri,
    form: UrlForm,
    uuidGenerator: F[UUID]): F[A] =
    uuidGenerator.flatMap(uuid =>
      client.expect[A](POST(form, auth_endpoint).putHeaders(`Idempotency-Key`(show"$uuid"))))

  final def wrap(client: Client[F]): Resource[F, Client[F]] =
    for {
      auth_token <- Resource.eval(getToken.flatMap(F.ref))
      // Background renewal loop. `renewToken` schedules the next fetch via its own `delayBy`
      // on the success path, but if it fails (network blip, decode error, short-lived token,
      // ...) that internal delay may never be reached. Without a floor here, a persistently
      // failing renewal would spin `foreverM` with zero delay, busy-looping the CPU and
      // hammering the auth endpoint. `handleErrorWith` swallows the failure but enforces a
      // minimum backoff before the loop retries, guaranteeing progress bounded from below.
      _ <- F.background[Nothing](
        renewToken(auth_token).handleErrorWith(_ => F.sleep(RENEW_FAILURE_BACKOFF)).foreverM)
      single_flight <- Resource.eval(SingleFlight[F, T])
    } yield Client[F] { request =>
      def allocate_response(token: T): F[(Response[F], Resource.ExitCase => F[Unit])] =
        client.run(withToken(token, request)).allocatedCase

      // `makeCaseFull` masks the handoff from each allocated response to this outer resource while
      // `poll` keeps response acquisition and token refresh cancelable. On 401, the first response
      // is finalized before the retry is acquired so a bounded connection pool can supply the retry.
      Resource.eval(auth_token.get).flatMap { token =>
        Resource
          .makeCaseFull[F, (Response[F], Resource.ExitCase => F[Unit])] { poll =>
            poll(allocate_response(token)).flatMap {
              case (response, release) if response.status === Status.Unauthorized =>
                release(Resource.ExitCase.Succeeded).flatMap(_ =>
                  poll(
                    single_flight(
                      auth_token.get.flatMap(refreshToken).flatTap(auth_token.set)
                    ).flatMap(allocate_response)))
              case allocated_response => F.pure(allocated_response)
            }
          } { case ((_, release), exit_case) => release(exit_case) }
          .map(_._1)
      }
    }
}
