package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.kernel.{Async, Deferred, Resource}
import cats.effect.std.Mutex
import cats.syntax.applicativeError.given
import cats.syntax.eq.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import org.http4s.Method.POST
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.{EntityDecoder, Request, Response, Status, Uri, UrlForm}

import scala.concurrent.duration.FiniteDuration

/** Wraps an HTTP client with authentication.
  *
  * @note
  *   An implementation may replay a request once after an authentication failure. Request entities supplied
  *   to the wrapped client must therefore be safe to evaluate again, and the target authentication layer
  *   should reject unauthorized requests before application side effects occur.
  */
trait Login[F[_]] {

  def login(businessClient: Client[F]): Resource[F, Client[F]]

  final def login(businessClient: Resource[F, Client[F]]): Resource[F, Client[F]] =
    businessClient.flatMap(login)

}

/** Provides token-based authentication for an HTTP client.
  *
  * Manages fetching, renewing, and applying tokens to requests. A token is renewed on two paths: proactively
  * on a schedule (`renewOnSchedule`, driven by `renewalDelay`) before it expires, and reactively when a
  * request returns `Unauthorized` (`renewOnRejection`), after which that request is replayed once. The
  * request entity must therefore be safely repeatable; a second `Unauthorized` response is returned without
  * another renewal or retry.
  *
  * Subclasses need to implement:
  *   - `getTokenFromCredentials`: how to obtain a token without using a current token
  *   - `renewOnRejection`: how to replace a token rejected by the protected resource
  *   - `renewOnSchedule`: how to replace a token proactively before it expires
  *   - `renewalDelay`: when to schedule renewal, or `None` to disable it
  *   - `withToken`: how to attach the token to an HTTP request
  *
  * ===`renewOnRejection` vs `renewOnSchedule`===
  *
  * The two token-replacement hooks correspond to the two paths that can replace a live token:
  *   - `renewOnRejection` runs on the *reactive* path: a request came back `Unauthorized`, so the current
  *     token is replaced and the request is replayed once.
  *   - `renewOnSchedule` runs on the *proactive* path: the background loop replaces the token on the
  *     `renewalDelay` schedule (and retries after `RENEW_FAILURE_BACKOFF` on failure) before it expires.
  *
  * Both hooks are abstract, so each flow states its two strategies explicitly. They may be the same function
  * when both paths use one grant, or differ when proactive renewal uses a different grant than rejected-token
  * replacement (e.g. renew via a stored `refresh_token` grant, but on a hard rejection fall back to a fresh
  * `getTokenFromCredentials`).
  */
abstract private class TokenAuthClient[F[_]](using F: Async[F]) extends Http4sClientDsl[F] {
  protected type T // token type

  final private case class TokenState(token: T, generation: Long, changed: Deferred[F, Unit])

  protected def getTokenFromCredentials: F[T]
  protected def renewOnRejection: T => F[T]
  protected def renewOnSchedule: T => F[T]
  protected def renewalDelay: T => Option[FiniteDuration]
  protected def withToken(token: T, req: Request[F]): Request[F]

  final protected def postToken[A: EntityDecoder[F, *]](
    client: Client[F],
    auth_endpoint: Uri,
    form: UrlForm): F[A] =
    client.expect[A](POST(form, auth_endpoint))

  final def wrap(client: Client[F]): Resource[F, Client[F]] =
    Resource.eval(
      getTokenFromCredentials.flatMap(token =>
        Deferred[F, Unit].flatMap(changed => F.ref(TokenState(token, 0L, changed))))
    ).flatMap { token_state_ref =>
      Resource.eval(Mutex[F]).flatMap { renewal_lock =>
        def replace_token(expected_generation: Long, replace: T => F[T]): F[TokenState] =
          renewal_lock.lock.surround {
            F.uncancelable { poll =>
              token_state_ref.get.flatMap { current =>
                if (current.generation === expected_generation)
                  poll(replace(current.token)).flatMap { token =>
                    Deferred[F, Unit].flatMap { changed =>
                      val updated = TokenState(token, current.generation + 1L, changed)
                      token_state_ref.set(updated)
                        .flatMap(_ => current.changed.complete(()))
                        .as(updated)
                    }
                  }
                else
                  F.pure(current)
              }
            }
          }

        def await_retry_or_change(scheduled: TokenState): F[Unit] =
          F.race(F.sleep(RENEW_FAILURE_BACKOFF), scheduled.changed.get).flatMap {
            case Left(_)  => renew_until_success(scheduled)
            case Right(_) => F.unit
          }

        def renew_until_success(scheduled: TokenState): F[Unit] =
          replace_token(scheduled.generation, renewOnSchedule)
            .flatMap(_ => F.unit)
            .handleErrorWith(_ => await_retry_or_change(scheduled))

        def renew_after_delay: F[Unit] =
          token_state_ref.get.flatMap { scheduled =>
            renewalDelay(scheduled.token) match {
              case Some(delay) =>
                F.race(F.sleep(delay), scheduled.changed.get).flatMap {
                  case Left(_)  => renew_until_success(scheduled)
                  case Right(_) => F.unit
                }
              case None => scheduled.changed.get
            }
          }

        F.background[Nothing](renew_after_delay.foreverM).map { _ =>
          Client[F] { request =>
            def allocate_response(token: T): F[(Response[F], Resource.ExitCase => F[Unit])] =
              client.run(withToken(token, request)).allocatedCase

            // `makeCaseFull` masks the handoff from each allocated response to this outer resource while
            // `poll` keeps response acquisition and token renewal cancelable. On 401, the first response
            // is finalized before the retry is acquired so a bounded connection pool can supply the retry.
            Resource.eval(token_state_ref.get).flatMap { requested =>
              Resource
                .makeCaseFull[F, (Response[F], Resource.ExitCase => F[Unit])] { poll =>
                  poll(allocate_response(requested.token)).flatMap {
                    case (response, release) if response.status === Status.Unauthorized =>
                      release(Resource.ExitCase.Succeeded).flatMap(_ =>
                        poll(replace_token(requested.generation, renewOnRejection)
                          .flatMap(current => allocate_response(current.token))))
                    case allocated_response => F.pure(allocated_response)
                  }
                } { case ((_, release), exit_case) => release(exit_case) }
                .map(_._1)
            }
          }
        }
      }
    }
}
