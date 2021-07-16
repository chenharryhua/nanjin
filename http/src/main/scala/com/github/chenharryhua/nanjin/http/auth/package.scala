package com.github.chenharryhua.nanjin.http

import org.http4s.client.middleware.RetryPolicy
import org.http4s.client.middleware.RetryPolicy.{exponentialBackoff, isErrorOrRetriableStatus}

import scala.concurrent.duration.DurationInt

package object auth {
  private[auth] def authPolicy[F[_]]: RetryPolicy[F] =
    RetryPolicy[F](exponentialBackoff(1.seconds, 8), (_, result) => isErrorOrRetriableStatus(result))
}
