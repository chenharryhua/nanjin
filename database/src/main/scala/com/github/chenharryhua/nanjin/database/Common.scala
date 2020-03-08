package com.github.chenharryhua.nanjin.database

import java.net.URI

final case class ConnectionString(value: String) extends AnyVal {
  def uri: URI = URI.create(value)
}

final case class DriverString(value: String) extends AnyVal
