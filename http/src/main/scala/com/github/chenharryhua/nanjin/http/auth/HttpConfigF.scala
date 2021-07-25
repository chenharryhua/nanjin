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

@Lenses final case class HttpParams(
  auth: AuthParams,
  maxRetries: Int,
  maxWait: FiniteDuration
) {
  def httpClient[F[_]](client: Client[F])(implicit F: Async[F]): Client[F] =
    Retry[F](RetryPolicy[F](exponentialBackoff(maxWait, maxRetries), (_, r) => isErrorOrRetriableStatus(r)))(
      Logger(auth.unsecureLog, auth.unsecureLog, _ => !auth.unsecureLog)(client))
}

object HttpParams {
  def apply(expiresIn: FiniteDuration): HttpParams = HttpParams(
    AuthParams(maxRetries = 8, maxWait = 5.second, tokenExpiresIn = expiresIn, unsecureLog = false),
    maxRetries = 0,
    maxWait = 1.seconds)
}

sealed private[auth] trait HttpConfigF[K]

private object HttpConfigF {
  implicit val functorAuthConfigF: Functor[HttpConfigF] = cats.derived.semiauto.functor

  final case class InitParams[K](expiresIn: FiniteDuration) extends HttpConfigF[K]
  final case class WithAuthMaxRetries[K](value: Int, cont: K) extends HttpConfigF[K]
  final case class WithAuthMaxWait[K](value: FiniteDuration, cont: K) extends HttpConfigF[K]
  final case class WithAuthTokenExpiresIn[K](value: FiniteDuration, cont: K) extends HttpConfigF[K]
  final case class WithUnsecureLog[K](value: Boolean, cont: K) extends HttpConfigF[K]

  final case class WithHttpMaxRetries[K](value: Int, cont: K) extends HttpConfigF[K]
  final case class WithHttpMaxWait[K](value: FiniteDuration, cont: K) extends HttpConfigF[K]

  val algebra: Algebra[HttpConfigF, HttpParams] = Algebra[HttpConfigF, HttpParams] {
    case InitParams(value)                   => HttpParams(value)
    case WithAuthMaxRetries(value, cont)     => HttpParams.auth.composeLens(AuthParams.maxRetries).set(value)(cont)
    case WithAuthMaxWait(value, cont)        => HttpParams.auth.composeLens(AuthParams.maxWait).set(value)(cont)
    case WithAuthTokenExpiresIn(value, cont) => HttpParams.auth.composeLens(AuthParams.tokenExpiresIn).set(value)(cont)
    case WithUnsecureLog(value, cont)        => HttpParams.auth.composeLens(AuthParams.unsecureLog).set(value)(cont)
    case WithHttpMaxWait(value, cont)        => HttpParams.maxWait.set(value)(cont)
    case WithHttpMaxRetries(value, cont)     => HttpParams.maxRetries.set(value)(cont)
  }
}

final private[auth] case class HttpConfig private (value: Fix[HttpConfigF]) {
  import HttpConfigF.*
  def withAuthMaxRetries(times: Int): HttpConfig       = HttpConfig(Fix(WithAuthMaxRetries(value = times, value)))
  def withAuthMaxWait(dur: FiniteDuration): HttpConfig = HttpConfig(Fix(WithAuthMaxWait(value = dur, value)))

  def withAuthTokenExpiresIn(dur: FiniteDuration): HttpConfig =
    HttpConfig(Fix(WithAuthTokenExpiresIn(value = dur, value)))

  def withUnsecureLogging: HttpConfig = HttpConfig(Fix(WithUnsecureLog(value = true, value)))

  def withHttpMaxRetries(times: Int): HttpConfig       = HttpConfig(Fix(WithHttpMaxRetries(value = times, value)))
  def withHttpMaxWait(dur: FiniteDuration): HttpConfig = HttpConfig(Fix(WithHttpMaxWait(value = dur, value)))

  def evalConfig: HttpParams = scheme.cata(algebra).apply(value)
}
private object HttpConfig {
  def apply(expiresIn: Option[FiniteDuration]): HttpConfig = HttpConfig(
    Fix(HttpConfigF.InitParams[Fix[HttpConfigF]](expiresIn.getOrElse(365.days))))
}
