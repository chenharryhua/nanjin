package com.github.chenharryhua.nanjin.http.client

sealed trait HttpException
final case class RetryableException() extends HttpException
final case class UnretryableException() extends HttpException
