package com.github.chenharryhua.nanjin.frontend

import com.raquo.laminar.api.L.Var
import io.circe.jawn.decode
import org.scalajs.dom
import org.scalajs.dom.{CloseEvent, Event, MessageEvent, WebSocket}

import scala.scalajs.js

final class WsConnector(port: Int, maxPoints: Int) {
  private val manager: ChartManager = new ChartManager(maxPoints)
  private val ws: WebSocket = new WebSocket(s"ws://localhost:$port/ws")

  def connect(chartVar: Var[Option[js.Dynamic]]): Unit = {

    ws.onopen = { (_: Event) =>
      val now = js.Date()
      val msg = s"WS connected at $now"
      dom.console.log(msg)
    }

    ws.onmessage = { (e: MessageEvent) =>
      ws.send("pong")
      decode[WsMessage](e.data.toString).toOption.foreach { msg =>
        manager.enqueue(msg).updateChart(chartVar)
      }
    }

    ws.onclose = { (c: CloseEvent) =>
      val now = js.Date()
      val cause = s"reason:${c.reason}, code:${c.code}, wasClean:${c.wasClean}"
      val msg = s"WS closed at $now, $cause"
      dom.console.log(msg)
    }
  }
}
