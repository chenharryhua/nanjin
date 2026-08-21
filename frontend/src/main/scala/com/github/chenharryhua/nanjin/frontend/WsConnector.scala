package com.github.chenharryhua.nanjin.frontend

import com.raquo.laminar.api.L.Var
import io.circe.jawn.decode
import org.scalajs.dom
import org.scalajs.dom.{CloseEvent, Event, MessageEvent, WebSocket}

import scala.compiletime.uninitialized
import scala.scalajs.js

final class WsConnector(config: FrontendConfig, maxPoints: Int) {
  private val manager: ChartManager = new ChartManager(maxPoints)
  private val wsUrl: String = s"${config.wsBaseUrl}/dashboard/ws"

  private val InitialDelayMs: Double = 1000
  private val MaxDelayMs: Double = 30000
  private val BackoffFactor: Double = 2.0

  val connected: Var[Boolean] = Var(false)

  private var ws: WebSocket = uninitialized
  private var chartVarRef: Var[Option[js.Dynamic]] = uninitialized
  private var currentDelay: Double = InitialDelayMs
  private var reconnectTimer: Int = 0
  private var disconnectTimer: Int = 0
  private var stopped: Boolean = false
  private var hasConnectedBefore: Boolean = false

  def connect(chartVar: Var[Option[js.Dynamic]]): Unit = {
    chartVarRef = chartVar
    openSocket()
  }

  def close(): Unit = {
    stopped = true
    if (disconnectTimer != 0) {
      dom.window.clearTimeout(disconnectTimer)
      disconnectTimer = 0
    }
    if (reconnectTimer != 0) {
      dom.window.clearTimeout(reconnectTimer)
      reconnectTimer = 0
    }
    if (ws != null && ws.readyState <= WebSocket.OPEN) ws.close()
  }

  private def openSocket(): Unit = {
    ws = new WebSocket(wsUrl)

    ws.onopen = { (_: Event) =>
      currentDelay = InitialDelayMs
      // Cancel pending disconnect banner — reconnected before user noticed
      if (disconnectTimer != 0) {
        dom.window.clearTimeout(disconnectTimer)
        disconnectTimer = 0
      }
      connected.set(true)
      // Only reset chart data on first connect.
      // Brief reconnects preserve the existing chart to avoid flicker.
      if (!hasConnectedBefore) {
        manager.reset(chartVarRef)
      }
      hasConnectedBefore = true
      dom.console.log(s"WS connected at ${new js.Date().toISOString()}")
    }

    ws.onmessage = { (e: MessageEvent) =>
      // Reply on every frame to keep intermediate proxies (ALB, nginx) from timing out idle connections
      ws.send("pong")
      decode[WsMessage](e.data.toString).toOption.foreach { msg =>
        manager.enqueue(msg).updateChart(chartVarRef)
      }
    }

    ws.onerror = { (_: Event) =>
      dom.console.warn("WS error occurred")
    }

    ws.onclose = { (c: CloseEvent) =>
      val cause = s"reason:${c.reason}, code:${c.code}, wasClean:${c.wasClean}"
      dom.console.log(s"WS closed at ${new js.Date().toISOString()}, $cause")
      // Delay showing "Disconnected" banner — if we reconnect within 2s, user won't notice
      if (disconnectTimer == 0) {
        disconnectTimer = dom.window.setTimeout(
          () => { disconnectTimer = 0; connected.set(false) },
          5000
        )
      }
      scheduleReconnect()
    }
  }

  private def scheduleReconnect(): Unit =
    if (!stopped) {
      dom.console.log(s"WS reconnecting in ${currentDelay.toLong}ms...")
      reconnectTimer = dom.window.setTimeout(
        () => {
          reconnectTimer = 0
          openSocket()
        },
        currentDelay
      )
      currentDelay = math.min(currentDelay * BackoffFactor, MaxDelayMs)
    }
}
