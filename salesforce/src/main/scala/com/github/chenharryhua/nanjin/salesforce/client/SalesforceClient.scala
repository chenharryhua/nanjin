package com.github.chenharryhua.nanjin.salesforce.client

import cats.MonadThrow
import cats.effect.IO
import cats.effect.kernel.Async
import org.http4s.blaze.client.BlazeClientBuilder

import scala.concurrent.ExecutionContext

final class SalesforceClient[F[_]: Async](ec: ExecutionContext) {

  val client = BlazeClientBuilder(ec).stream
  IO(1) <* IO.never >> IO.canceled
}
