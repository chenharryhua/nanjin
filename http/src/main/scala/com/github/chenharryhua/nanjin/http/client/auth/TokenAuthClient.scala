package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.kernel.{Async, Deferred, Resource}
import cats.effect.std.Mutex
import cats.syntax.applicativeError.given
import cats.syntax.eq.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.{Request, Response, Status}

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
  *   - `renewOnSchedule` runs on the *proactive* path: the background loop replaces the token once after the
  *     `renewalDelay` elapses. A failure does not retry the token request; the loop waits until another path
  *     successfully replaces the token, then schedules that new token. Callers that want token-endpoint retry
  *     behavior configure it on the supplied authentication client.
  *
  * Both hooks are abstract, so each flow states its two strategies explicitly. They may be the same function
  * when both paths use one grant, or differ when proactive renewal uses a different grant than rejected-token
  * replacement (e.g. renew via a stored `refresh_token` grant, but on a hard rejection fall back to a fresh
  * `getTokenFromCredentials`).
  *
  * This coordinator does not retry token-endpoint requests. Initial-acquisition and reactive-renewal failures
  * propagate to the operation waiting for them; a scheduled-renewal failure waits silently for another path
  * to replace the token. Callers that need retries or failure observability configure those concerns on the
  * supplied authentication client.
  */
abstract private class TokenAuthClient[F[_]](using F: Async[F]) extends Http4sClientDsl[F] {
  protected type T // token type

  /** A snapshot of the current token together with the bookkeeping that coordinates its replacement.
    *
    * @param token
    *   the token currently applied to outgoing requests.
    * @param generation
    *   a monotonically increasing counter, starting at `0` and incremented by one on every replacement. It is
    *   the compare key for replacement: `replace_token` swaps the token only when the caller's expected
    *   generation still matches the live one, so a replacement racing a concurrent one (e.g. a scheduled
    *   renewal against a `401` renewal) that lost the race becomes a no-op instead of overwriting the winner.
    * @param changed
    *   completed exactly once, when this state is superseded by the next one. The scheduled-renewal loop
    *   races its sleep against `changed.get` and waits on it after a failed scheduled attempt, so a
    *   successful replacement on another path abandons the obsolete schedule and starts scheduling the new
    *   token.
    */
  final private case class TokenState(token: T, generation: Long, changed: Deferred[F, Unit])

  protected def getTokenFromCredentials: F[T]
  protected def renewOnRejection(token: T): F[T]
  protected def renewOnSchedule(token: T): F[T]
  protected def renewalDelay(token: T): Option[FiniteDuration]
  protected def withToken(token: T, req: Request[F]): Request[F]

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

        /** Runs one iteration of the proactive-renewal scheduler against a snapshot of the current token
          * state.
          *
          * If `renewalDelay` returns `Some`, the delay races the snapshot's `changed` signal. A replacement
          * on another path wins that race and ends this iteration without renewal; if the delay wins,
          * `renewOnSchedule` is attempted once through `replace_token`. A failed attempt is not retried here:
          * it waits for another successful replacement to complete `changed`, after which the surrounding
          * `foreverM` starts a new iteration and schedules that token. If `renewalDelay` returns `None`, this
          * iteration likewise waits for the next successful replacement before continuing.
          */
        def schedule_renewal: F[Unit] =
          token_state_ref.get.flatMap { scheduled =>
            renewalDelay(scheduled.token) match {
              case Some(delay) =>
                F.race(F.sleep(delay), scheduled.changed.get).flatMap {
                  case Left(_) =>
                    replace_token(scheduled.generation, renewOnSchedule)
                      .void
                      .handleErrorWith(_ => scheduled.changed.get)
                  case Right(_) => F.unit
                }
              case None => scheduled.changed.get
            }
          }

        F.background[Nothing](schedule_renewal.foreverM).map { _ =>
          Client[F] { request =>
            def allocate_response(token: T): F[(Response[F], Resource.ExitCase => F[Unit])] =
              client.run(withToken(token, request)).allocatedCase

            // `makeCaseFull` masks the handoff from each allocated response to this outer resource while
            // `poll` keeps response acquisition and token renewal cancelable. On 401, the first response
            // is finalized before the retry is acquired so a bounded connection pool can supply the retry.
            Resource.eval(token_state_ref.get).flatMap { state =>
              Resource
                .makeCaseFull[F, (Response[F], Resource.ExitCase => F[Unit])] { poll =>
                  poll(allocate_response(state.token)).flatMap {
                    case (response, release) if response.status === Status.Unauthorized =>
                      release(Resource.ExitCase.Succeeded).flatMap(_ =>
                        poll(replace_token(state.generation, renewOnRejection)
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
