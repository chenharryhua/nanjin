package com.github.chenharryhua.nanjin.http.auth

import cats.Functor
import cats.effect.kernel.Temporal
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.http4s.client.Client
import org.http4s.client.middleware.RetryPolicy.{exponentialBackoff, isErrorOrRetriableStatus}
import org.http4s.client.middleware.{Retry, RetryPolicy}

import scala.concurrent.duration.*

@Lenses final case class AuthParams(maxRetries: Int, maxWait: FiniteDuration, expiresIn: FiniteDuration) {
  def offset: FiniteDuration = maxWait.mul(maxRetries.toLong)
  def authClient[F[_]](client: Client[F])(implicit F: Temporal[F]): Client[F] =
    Retry[F](RetryPolicy[F](exponentialBackoff(maxWait, maxRetries), (_, r) => isErrorOrRetriableStatus(r)))(client)
}

object AuthParams {
  def apply(expiresIn: FiniteDuration): AuthParams =
    AuthParams(maxRetries = 8, maxWait = 5.second, expiresIn = expiresIn)
}

sealed private[auth] trait AuthConfigF[K]

private object AuthConfigF {
  implicit val functorAuthConfigF: Functor[AuthConfigF] = cats.derived.semiauto.functor

  final case class InitParams[K](expiresIn: FiniteDuration) extends AuthConfigF[K]
  final case class WithAuthMaxRetries[K](value: Int, cont: K) extends AuthConfigF[K]
  final case class WithAuthMaxWait[K](value: FiniteDuration, cont: K) extends AuthConfigF[K]
  final case class WithAuthExpiresIn[K](value: FiniteDuration, cont: K) extends AuthConfigF[K]

  val algebra: Algebra[AuthConfigF, AuthParams] = Algebra[AuthConfigF, AuthParams] {
    case InitParams(value)               => AuthParams(value)
    case WithAuthMaxRetries(value, cont) => AuthParams.maxRetries.set(value)(cont)
    case WithAuthMaxWait(value, cont)    => AuthParams.maxWait.set(value)(cont)
    case WithAuthExpiresIn(value, cont)  => AuthParams.expiresIn.set(value)(cont)
  }
}

final private[auth] case class AuthConfig private (value: Fix[AuthConfigF]) {
  import AuthConfigF.*
  def withAuthMaxRetries(times: Int): AuthConfig       = AuthConfig(Fix(WithAuthMaxRetries(value = times, value)))
  def withAuthMaxWait(dur: FiniteDuration): AuthConfig = AuthConfig(Fix(WithAuthMaxWait(value = dur, value)))

  def withAuthExpiresIn(dur: FiniteDuration): AuthConfig = AuthConfig(Fix(WithAuthExpiresIn(value = dur, value)))

  def evalConfig: AuthParams = scheme.cata(algebra).apply(value)
}
private object AuthConfig {
  def apply(expiresIn: FiniteDuration): AuthConfig = AuthConfig(
    Fix(AuthConfigF.InitParams[Fix[AuthConfigF]](expiresIn)))
}
