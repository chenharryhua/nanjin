package com.github.chenharryhua.nanjin.frontend

import org.scalajs.dom

final case class FrontendConfig(
  wsBaseUrl: String,
  apiBaseUrl: String
)

object FrontendConfig {

  def fromWindow(): FrontendConfig = {

    val protocol = dom.window.location.protocol

    val wsScheme = if (protocol == "https:") "wss" else "ws"

    val host = dom.window.location.host
    val origin = dom.window.location.origin

    FrontendConfig(
      wsBaseUrl = s"$wsScheme://$host",
      apiBaseUrl = origin
    )
  }
}
