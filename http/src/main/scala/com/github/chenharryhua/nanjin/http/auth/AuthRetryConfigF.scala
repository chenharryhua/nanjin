package com.github.chenharryhua.nanjin.http.auth

import cats.Functor
import cats.effect.kernel.Temporal
import higherkindness.droste.{scheme, Algebra}
import higherkindness.droste.data.Fix
import monocle.macros.Lenses
import org.http4s.client.Client
import org.http4s.client.middleware.RetryPolicy.{exponentialBackoff, isErrorOrRetriableStatus}
import org.http4s.client.middleware.{Retry, RetryPolicy}

import scala.concurrent.duration.*

@Lenses final case class AuthRetryParams(maxRetries: Int, maxWait: FiniteDuration) {
  def offset(expireIn: FiniteDuration): FiniteDuration = expireIn - (maxWait * maxRetries.toLong)
  def retriableClient[F[_]](client: Client[F])(implicit F: Temporal[F]): Client[F] =
    Retry[F](RetryPolicy[F](exponentialBackoff(maxWait, maxRetries), (_, result) => isErrorOrRetriableStatus(result)))(
      client)
}

object AuthRetryParams {
  def apply(): AuthRetryParams = AuthRetryParams(maxRetries = 8, maxWait = 5.second)
}

sealed trait AuthRetryConfigF[K]

object AuthRetryConfigF {
  implicit val functorAuthRetryConfigF: Functor[AuthRetryConfigF] = cats.derived.semiauto.functor

  final case class InitParams[K]() extends AuthRetryConfigF[K]
  final case class WithMaxRetries[K](value: Int, cont: K) extends AuthRetryConfigF[K]
  final case class WithMaxWait[K](value: FiniteDuration, cont: K) extends AuthRetryConfigF[K]
  val algebra: Algebra[AuthRetryConfigF, AuthRetryParams] = Algebra[AuthRetryConfigF, AuthRetryParams] {
    case InitParams()                => AuthRetryParams()
    case WithMaxRetries(value, cont) => AuthRetryParams.maxRetries.set(value)(cont)
    case WithMaxWait(value, cont)    => AuthRetryParams.maxWait.set(value)(cont)
  }
}

final case class AuthRetryConfig private (value: Fix[AuthRetryConfigF]) {
  import AuthRetryConfigF.*
  def withMaxRetries(times: Int): AuthRetryConfig       = AuthRetryConfig(Fix(WithMaxRetries(value = times, value)))
  def withMaxWait(dur: FiniteDuration): AuthRetryConfig = AuthRetryConfig(Fix(WithMaxWait(value = dur, value)))

  def evalConfig: AuthRetryParams = scheme.cata(algebra).apply(value)
}
object AuthRetryConfig {
  def apply(): AuthRetryConfig = AuthRetryConfig(Fix(AuthRetryConfigF.InitParams[Fix[AuthRetryConfigF]]()))
}
