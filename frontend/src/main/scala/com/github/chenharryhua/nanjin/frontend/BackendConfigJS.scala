package com.github.chenharryhua.nanjin.frontend

import scala.scalajs.js

/*
 * Backend Config defined at:
 * `com.github.chenharryhua.nanjin.guard.service.dashboard.BackendConfig`
 */

@js.native
trait BackendConfigJS extends js.Object {
  val serviceName: String
  val port: Int
  val zoneId: String
  val maxPoints: Int
  val policy: String
}

object BackendConfigJS {
  def apply(): BackendConfigJS =
    js.Dynamic.global.BACKEND_CONFIG.asInstanceOf[BackendConfigJS]
}

case class BackendConfig(serviceName: String, port: Int, zoneId: String, maxPoints: Int, policy: String)

object BackendConfig {
  def load(): BackendConfig = {
    val jsCfg = BackendConfigJS()
    BackendConfig(
      serviceName = jsCfg.serviceName,
      port = jsCfg.port,
      zoneId = jsCfg.zoneId,
      maxPoints = jsCfg.maxPoints,
      policy = jsCfg.policy
    )
  }
}
