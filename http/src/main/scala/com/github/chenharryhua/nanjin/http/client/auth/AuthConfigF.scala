package com.github.chenharryhua.nanjin.http.client.auth

import cats.Functor
import cats.effect.Async
import cats.kernel.Order
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.http4s.client.Client
import org.http4s.client.middleware.RetryPolicy.{exponentialBackoff, isErrorOrRetriableStatus}
import org.http4s.client.middleware.{Logger, Retry, RetryPolicy}

import scala.concurrent.duration.*

@Lenses final case class AuthParams(maxRetries: Int, maxWait: FiniteDuration, unsecureLog: Boolean) {
  private val preact: FiniteDuration = maxWait * maxRetries.toLong

  def dormant(delay: FiniteDuration): FiniteDuration = Order.max(delay - preact, Duration.Zero)

  def authClient[F[_]](client: Client[F])(implicit F: Async[F]): Client[F] =
    Retry[F](RetryPolicy[F](exponentialBackoff(maxWait, maxRetries), (_, r) => isErrorOrRetriableStatus(r)))(
      Logger(unsecureLog, unsecureLog, _ => !unsecureLog)(client))
}

object AuthParams {
  def apply(): AuthParams = AuthParams(maxRetries = 10, maxWait = 6.seconds, unsecureLog = false)
}

sealed private[auth] trait AuthConfigF[K]

private object AuthConfigF {
  implicit val functorAuthConfigF: Functor[AuthConfigF] = cats.derived.semiauto.functor

  final case class InitParams[K]() extends AuthConfigF[K]
  final case class WithAuthMaxRetries[K](value: Int, cont: K) extends AuthConfigF[K]
  final case class WithAuthMaxWait[K](value: FiniteDuration, cont: K) extends AuthConfigF[K]
  final case class WithAuthInsecureLog[K](value: Boolean, cont: K) extends AuthConfigF[K]

  val algebra: Algebra[AuthConfigF, AuthParams] = Algebra[AuthConfigF, AuthParams] {
    case InitParams()                     => AuthParams()
    case WithAuthMaxRetries(value, cont)  => AuthParams.maxRetries.replace(value)(cont)
    case WithAuthMaxWait(value, cont)     => AuthParams.maxWait.replace(value)(cont)
    case WithAuthInsecureLog(value, cont) => AuthParams.unsecureLog.replace(value)(cont)
  }
}

final private[auth] case class AuthConfig(value: Fix[AuthConfigF]) {
  import AuthConfigF.*
  def withAuthMaxRetries(times: Int): AuthConfig = AuthConfig(Fix(WithAuthMaxRetries(value = times, value)))
  def withAuthMaxWait(dur: FiniteDuration): AuthConfig = AuthConfig(Fix(WithAuthMaxWait(value = dur, value)))

  def withAuthInsecureLogging: AuthConfig = AuthConfig(Fix(WithAuthInsecureLog(value = true, value)))

  def evalConfig: AuthParams = scheme.cata(algebra).apply(value)
}
private object AuthConfig {
  def apply(): AuthConfig = AuthConfig(Fix(AuthConfigF.InitParams[Fix[AuthConfigF]]()))
}
