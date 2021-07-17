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

@Lenses final case class AuthParams(maxRetries: Int, maxWait: FiniteDuration) {
  def offset(expireIn: FiniteDuration): FiniteDuration = expireIn - (maxWait * maxRetries.toLong)
  def authClient[F[_]](client: Client[F])(implicit F: Temporal[F]): Client[F] =
    Retry[F](RetryPolicy[F](exponentialBackoff(maxWait, maxRetries), (_, result) => isErrorOrRetriableStatus(result)))(
      client)
}

object AuthParams {
  def apply(): AuthParams = AuthParams(maxRetries = 8, maxWait = 5.second)
}

sealed private[auth] trait AuthConfigF[K]

private object AuthConfigF {
  implicit val functorAuthConfigF: Functor[AuthConfigF] = cats.derived.semiauto.functor

  final case class InitParams[K]() extends AuthConfigF[K]
  final case class WithMaxRetries[K](value: Int, cont: K) extends AuthConfigF[K]
  final case class WithMaxWait[K](value: FiniteDuration, cont: K) extends AuthConfigF[K]
  val algebra: Algebra[AuthConfigF, AuthParams] = Algebra[AuthConfigF, AuthParams] {
    case InitParams()                => AuthParams()
    case WithMaxRetries(value, cont) => AuthParams.maxRetries.set(value)(cont)
    case WithMaxWait(value, cont)    => AuthParams.maxWait.set(value)(cont)
  }
}

final private[auth] case class AuthConfig private (value: Fix[AuthConfigF]) {
  import AuthConfigF.*
  def withMaxRetries(times: Int): AuthConfig       = AuthConfig(Fix(WithMaxRetries(value = times, value)))
  def withMaxWait(dur: FiniteDuration): AuthConfig = AuthConfig(Fix(WithMaxWait(value = dur, value)))

  def evalConfig: AuthParams = scheme.cata(algebra).apply(value)
}
private object AuthConfig {
  def apply(): AuthConfig = AuthConfig(Fix(AuthConfigF.InitParams[Fix[AuthConfigF]]()))
}
