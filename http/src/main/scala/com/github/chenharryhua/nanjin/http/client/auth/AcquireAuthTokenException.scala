package com.github.chenharryhua.nanjin.http.client.auth

final case class AcquireAuthTokenException(rootCause: Throwable) extends Exception(rootCause)
