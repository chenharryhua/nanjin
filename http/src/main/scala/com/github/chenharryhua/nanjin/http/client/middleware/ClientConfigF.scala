package com.github.chenharryhua.nanjin.http.client.middleware

import cats.Functor
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.{policies, Policy}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.syntax.all.*
import org.http4s.client.Client
import org.http4s.client.middleware.*
import org.typelevel.ci.CIString

import java.net.CookieManager
import java.time.ZoneId

sealed trait ClientConfigF[A]

final case class LogParams(logHeader: Boolean, logBody: Boolean, redact: CIString => Boolean)

final case class ClientParams(
  policy: Policy,
  zoneId: ZoneId,
  cookie: Option[Either[Unit, CookieManager]],
  logParams: LogParams) {

  def client[F[_]: Async](clientR: Resource[F, Client[F]]): Resource[F, Client[F]] = {

    val logger: Client[F] => Client[F] =
      if (logParams.logHeader || logParams.logBody)
        Logger(logParams.logHeader, logParams.logBody, logParams.redact)(_)
      else identity

    val cooker: Client[F] => F[Client[F]] = cookie match {
      case Some(value) =>
        value match {
          case Left(_)      => CookieJar.impl[F]
          case Right(value) => cookieBox(value)(_).pure[F]
        }
      case None => identity(_).pure[F]
    }

    clientR.map(logger).map(retry(policy, zoneId)).evalMap(cooker)
  }
}

object ClientParams {
  def apply(zoneId: ZoneId): ClientParams = ClientParams(
    policy = policies.giveUp,
    zoneId = zoneId,
    cookie = None,
    logParams = LogParams(logHeader = false, logBody = false, redact = Logger.defaultRedactHeadersWhen)
  )
}

object ClientConfigF {
  implicit val clientConfigFunctor: Functor[ClientConfigF] = cats.derived.semiauto.functor

  final case class InitParams[K](zoneId: ZoneId) extends ClientConfigF[K]

  final case class WithRetryPolicy[K](policy: Policy, cont: K) extends ClientConfigF[K]

  final case class WithCookie[K](cookieManager: Option[Either[Unit, CookieManager]], cont: K)
      extends ClientConfigF[K]

  final case class WithLogHeader[K](value: Boolean, cont: K) extends ClientConfigF[K]
  final case class WithLogBody[K](value: Boolean, cont: K) extends ClientConfigF[K]
  final case class WithLogReact[K](redact: CIString => Boolean, cont: K) extends ClientConfigF[K]

  val algebra: Algebra[ClientConfigF, ClientParams] = Algebra {
    case InitParams(zoneId)            => ClientParams(zoneId)
    case WithRetryPolicy(policy, cont) => cont.focus(_.policy).replace(policy)
    case WithCookie(cookie, cont)      => cont.focus(_.cookie).replace(cookie)
    case WithLogHeader(value, cont)    => cont.focus(_.logParams.logHeader).replace(value)
    case WithLogBody(value, cont)      => cont.focus(_.logParams.logBody).replace(value)
    case WithLogReact(redact, cont)    => cont.focus(_.logParams.redact).replace(redact)
  }
}

final case class ClientConfig(config: Fix[ClientConfigF]) {
  import ClientConfigF.*
  def withPolicy(policy: Policy): ClientConfig = ClientConfig(Fix(WithRetryPolicy(policy, config)))

  def withCookie: ClientConfig = ClientConfig(Fix(WithCookie(Some(Left(())), config)))
  def withCookie(manager: CookieManager): ClientConfig =
    ClientConfig(Fix(WithCookie(Some(Right(manager)), config)))

  def withHeaderLog: ClientConfig = ClientConfig(Fix(WithLogHeader(value = true, config)))
  def withBodyLog: ClientConfig   = ClientConfig(Fix(WithLogBody(value = true, config)))
  def withLog: ClientConfig       = this.withHeaderLog.withBodyLog
  def withRedact(f: CIString => Boolean): ClientConfig = ClientConfig(Fix(WithLogReact(f, config)))

  lazy val params: ClientParams = scheme.cata(algebra).apply(config)
}
object ClientConfig {
  def apply(zoneId: ZoneId): ClientConfig =
    ClientConfig(Fix(ClientConfigF.InitParams[Fix[ClientConfigF]](zoneId)))
}
