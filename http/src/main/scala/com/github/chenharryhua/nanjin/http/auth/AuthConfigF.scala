package com.github.chenharryhua.nanjin.http.auth

import cats.Functor
import cats.effect.Async
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.http4s.client.Client
import org.http4s.client.middleware.RetryPolicy.{exponentialBackoff, isErrorOrRetriableStatus}
import org.http4s.client.middleware.{Logger, Retry, RetryPolicy}

import scala.concurrent.duration.*

@Lenses final case class AuthParams(
  maxRetries: Int,
  maxWait: FiniteDuration,
  tokenExpiresIn: FiniteDuration,
  logHeader: Boolean,
  logBody: Boolean) {
  def delay(tokenExpire: Option[FiniteDuration]): FiniteDuration =
    tokenExpire.filter(_ < tokenExpiresIn).getOrElse(tokenExpiresIn) - maxWait * maxRetries.toLong

  def authClient[F[_]](client: Client[F])(implicit F: Async[F]): Client[F] =
    Retry[F](RetryPolicy[F](exponentialBackoff(maxWait, maxRetries), (_, r) => isErrorOrRetriableStatus(r)))(
      Logger(logHeader, logBody)(client))
}

object AuthParams {
  def apply(expiresIn: FiniteDuration): AuthParams =
    AuthParams(maxRetries = 8, maxWait = 5.second, tokenExpiresIn = expiresIn, logHeader = false, logBody = false)
}

sealed private[auth] trait AuthConfigF[K]

private object AuthConfigF {
  implicit val functorAuthConfigF: Functor[AuthConfigF] = cats.derived.semiauto.functor

  final case class InitParams[K](expiresIn: FiniteDuration) extends AuthConfigF[K]
  final case class WithAuthMaxRetries[K](value: Int, cont: K) extends AuthConfigF[K]
  final case class WithAuthMaxWait[K](value: FiniteDuration, cont: K) extends AuthConfigF[K]
  final case class WithAuthTokenExpiresIn[K](value: FiniteDuration, cont: K) extends AuthConfigF[K]
  final case class WithAuthHeaderLog[K](value: Boolean, cont: K) extends AuthConfigF[K]
  final case class WithAuthBodyLog[K](value: Boolean, cont: K) extends AuthConfigF[K]
  val algebra: Algebra[AuthConfigF, AuthParams] = Algebra[AuthConfigF, AuthParams] {
    case InitParams(value)                   => AuthParams(value)
    case WithAuthMaxRetries(value, cont)     => AuthParams.maxRetries.set(value)(cont)
    case WithAuthMaxWait(value, cont)        => AuthParams.maxWait.set(value)(cont)
    case WithAuthTokenExpiresIn(value, cont) => AuthParams.tokenExpiresIn.set(value)(cont)
    case WithAuthHeaderLog(value, cont)      => AuthParams.logHeader.set(value)(cont)
    case WithAuthBodyLog(value, cont)        => AuthParams.logBody.set(value)(cont)
  }
}

final private[auth] case class AuthConfig private (value: Fix[AuthConfigF]) {
  import AuthConfigF.*
  def withAuthMaxRetries(times: Int): AuthConfig       = AuthConfig(Fix(WithAuthMaxRetries(value = times, value)))
  def withAuthMaxWait(dur: FiniteDuration): AuthConfig = AuthConfig(Fix(WithAuthMaxWait(value = dur, value)))

  def withAuthTokenExpiresIn(dur: FiniteDuration): AuthConfig =
    AuthConfig(Fix(WithAuthTokenExpiresIn(value = dur, value)))

  def withUnsecureLoggingAuthHeader: AuthConfig = AuthConfig(Fix(WithAuthHeaderLog(value = true, value)))
  def withUnsecureLoggingAuthBody: AuthConfig   = AuthConfig(Fix(WithAuthBodyLog(value = true, value)))

  def evalConfig: AuthParams = scheme.cata(algebra).apply(value)
}
private object AuthConfig {
  def apply(expiresIn: Option[FiniteDuration]): AuthConfig = AuthConfig(
    Fix(AuthConfigF.InitParams[Fix[AuthConfigF]](expiresIn.getOrElse(365.days))))
}
