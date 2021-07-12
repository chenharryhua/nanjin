package com.github.chenharryhua.nanjin.http.salesforce.client

import cats.Functor
import higherkindness.droste.Algebra
import higherkindness.droste.data.Fix
import monocle.macros.Lenses

import scala.concurrent.duration.*

@Lenses final case class Authentication(maxRetries: Int, interval: FiniteDuration)
@Lenses final case class SalesforceParams(auth: Authentication)

object SalesforceParams {
  def apply(): SalesforceParams = SalesforceParams(Authentication(10, 5.seconds))
}

sealed private[salesforce] trait SalesforceConfigF[A]

object SalesforceConfigF {
  implicit val functorSalesforceConfigF: Functor[SalesforceConfigF] = cats.derived.semiauto.functor[SalesforceConfigF]

  final case class WithAuthenticationMaxRetries[K](value: Int, cont: K) extends SalesforceConfigF[K]
  final case class WithAuthenticationInterval[K](value: FiniteDuration, cont: K) extends SalesforceConfigF[K]

  val algebra: Algebra[SalesforceConfigF, SalesforceParams] = Algebra[SalesforceConfigF, SalesforceParams] {
    case WithAuthenticationMaxRetries(v, c) => SalesforceParams.auth.composeLens(Authentication.maxRetries).set(v)(c)
    case WithAuthenticationInterval(v, c)   => SalesforceParams.auth.composeLens(Authentication.interval).set(v)(c)

  }
}

final case class SalesforceConfig(value: Fix[SalesforceConfigF]) {}
