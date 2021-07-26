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
  unsecureLog: Boolean) {
  def delay(tokenExpire: Option[FiniteDuration]): FiniteDuration =
    tokenExpire.filter(_ < tokenExpiresIn).getOrElse(tokenExpiresIn) - maxWait * maxRetries.toLong

  def authClient[F[_]](client: Client[F])(implicit F: Async[F]): Client[F] =
    Retry[F](RetryPolicy[F](exponentialBackoff(maxWait, maxRetries), (_, r) => isErrorOrRetriableStatus(r)))(
      Logger(unsecureLog, unsecureLog, _ => !unsecureLog)(client))
}

object AuthParams {
  def apply(expiresIn: FiniteDuration): AuthParams =
    AuthParams(maxRetries = 10, maxWait = 6.seconds, tokenExpiresIn = expiresIn, unsecureLog = false)
}

sealed private[auth] trait AuthConfigF[K]

private object AuthConfigF {
  implicit val functorAuthConfigF: Functor[AuthConfigF] = cats.derived.semiauto.functor

  final case class InitParams[K](expiresIn: FiniteDuration) extends AuthConfigF[K]
  final case class WithAuthMaxRetries[K](value: Int, cont: K) extends AuthConfigF[K]
  final case class WithAuthMaxWait[K](value: FiniteDuration, cont: K) extends AuthConfigF[K]
  final case class WithAuthTokenExpiresIn[K](value: FiniteDuration, cont: K) extends AuthConfigF[K]
  final case class WithUnsecureLog[K](value: Boolean, cont: K) extends AuthConfigF[K]

  val algebra: Algebra[AuthConfigF, AuthParams] = Algebra[AuthConfigF, AuthParams] {
    case InitParams(value)                   => AuthParams(value)
    case WithAuthMaxRetries(value, cont)     => AuthParams.maxRetries.set(value)(cont)
    case WithAuthMaxWait(value, cont)        => AuthParams.maxWait.set(value)(cont)
    case WithAuthTokenExpiresIn(value, cont) => AuthParams.tokenExpiresIn.set(value)(cont)
    case WithUnsecureLog(value, cont)        => AuthParams.unsecureLog.set(value)(cont)
  }
}

final private[auth] case class HttpConfig private (value: Fix[AuthConfigF]) {
  import AuthConfigF.*
  def withAuthMaxRetries(times: Int): HttpConfig       = HttpConfig(Fix(WithAuthMaxRetries(value = times, value)))
  def withAuthMaxWait(dur: FiniteDuration): HttpConfig = HttpConfig(Fix(WithAuthMaxWait(value = dur, value)))

  def withAuthTokenExpiresIn(dur: FiniteDuration): HttpConfig =
    HttpConfig(Fix(WithAuthTokenExpiresIn(value = dur, value)))

  def withUnsecureLogging: HttpConfig = HttpConfig(Fix(WithUnsecureLog(value = true, value)))

  def evalConfig: AuthParams = scheme.cata(algebra).apply(value)
}
private object HttpConfig {
  def apply(expiresIn: Option[FiniteDuration]): HttpConfig = HttpConfig(
    Fix(AuthConfigF.InitParams[Fix[AuthConfigF]](expiresIn.getOrElse(365.days))))
}
