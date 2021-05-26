package com.github.chenharryhua.nanjin.salesforce.client

import cats.MonadThrow
import cats.effect.{IO, Ref}
import cats.effect.kernel.Async
import org.http4s.Credentials.Token
import org.http4s.blaze.client.BlazeClientBuilder

import scala.concurrent.ExecutionContext

final class SalesforceClient[F[_]: Async](builder: BlazeClientBuilder[F], token: Ref[F, Token]) {

  builder.stream
}
