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
  *   - `getToken`: how to obtain a new token
  *   - `renewToken`: how to refresh or schedule token renewal
  *   - `withToken`: how to attach the token to an HTTP request
  */
abstract private class TokenAuthClient[F[_], T](using F: Async[F]) extends Http4sClientDsl[F] {
  protected def getToken: F[T]
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
      authToken <- Resource.eval(getToken.flatMap(F.ref))
      // Background renewal loop. `renewToken` schedules the next fetch via its own `delayBy`
      // on the success path, but if it fails (network blip, decode error, short-lived token,
      // ...) that internal delay may never be reached. Without a floor here, a persistently
      // failing renewal would spin `foreverM` with zero delay, busy-looping the CPU and
      // hammering the auth endpoint. `handleErrorWith` swallows the failure but enforces a
      // minimum backoff before the loop retries, guaranteeing progress bounded from below.
      _ <- F.background[Nothing](
        renewToken(authToken).handleErrorWith(_ => F.sleep(renewFailureBackoff)).foreverM)
      singleFlight <- Resource.eval(SingleFlight[F, T])
    } yield Client[F] { request =>
      def runWithToken(token: T): Resource[F, Response[F]] =
        client.run(withToken(token, request))

      Resource.eval(authToken.get).flatMap { token =>
        runWithToken(token).flatMap { response =>
          if (response.status === Status.Unauthorized)
            Resource.eval(singleFlight(getToken.flatTap(authToken.set))).flatMap(runWithToken)
          else
            Resource.pure(response)
        }
      }
    }
}
