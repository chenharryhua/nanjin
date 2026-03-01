package com.github.chenharryhua.nanjin.frontend
import com.raquo.laminar.api.L._
import com.raquo.laminar.nodes.ReactiveHtmlElement
import io.circe.jawn.decode
import org.scalajs.dom
import org.scalajs.dom.{html, HTMLCanvasElement, HTMLDivElement, MessageEvent, WebSocket}

import scala.scalajs.js
import scala.scalajs.js.Dynamic.literal

object Main {

  // hold chart instance
  private val chartVar: Var[Option[js.Dynamic]] = Var(Option.empty[js.Dynamic])

  // ---- init chart ----
  private def initChart(canvas: HTMLCanvasElement, zoneId: String): js.Dynamic = {
    val ctx = canvas.getContext("2d")

    js.Dynamic.newInstance(js.Dynamic.global.Chart)(
      ctx,
      literal(
        `type` = "line",
        data = literal(datasets = js.Array()),
        options = literal(
          parsing = false, // IMPORTANT for {x,y}
          responsive = true,
          animation = false,
          scales = literal(
            x = literal(
              `type` = "time",
              time = literal(unit = "minute", tooltipFormat = "HH:mm"),
              title = literal(display = true, text = "Time"),
              adapters = js.Dynamic.literal(date = js.Dynamic.literal(zone = zoneId))
            ),
            y = literal(
              beginAtZero = true,
              title = literal(display = true, text = "Value"),
              ticks = js.Dynamic.literal(
                precision = 0,
                callback = (value: js.Any) => {
                  val v = value.asInstanceOf[Double]
                  Math.round(v).toString
                }
              )
            )
          ),
          plugins = literal(legend = literal(display = true))
        )
      )
    )
  }

  // ---- connect websocket ----
  private def connectWS(port: String): Unit = {

    val ws = new WebSocket(s"ws://localhost:$port/ws")

    ws.onopen = _ => dom.console.log("WS connected")

    ws.onmessage = { (e: MessageEvent) =>
      ws.send("pong")
      decode[WsMessage](e.data.toString).toOption.foreach { msg =>
        chartVar.now().foreach { chart =>
          appendToChart(chart, msg)
        }
      }
    }

    ws.onclose = _ => dom.console.log("WS closed")
  }

  // ---- UI ----
  private val app: ReactiveHtmlElement[HTMLDivElement] = {
    val rootDiv = dom.document.getElementById("chart-root").asInstanceOf[html.Div]
    val port: String = rootDiv.dataset.get("ws-port").getOrElse("1026")
    val zoneId: String = rootDiv.dataset.get("zone-id").getOrElse("UTC")

    div(
      h2("Realtime Metrics"),
      canvasTag(
        width  := "80%",
        height := "70%",
        cls    := "chart-canvas",

        onMountCallback { ctx =>
          val canvas = ctx.thisNode.ref

          val chart = initChart(canvas, zoneId)
          chartVar.set(Some(chart))

          connectWS(port)
        },

        // optional cleanup
        onUnmountCallback { _ =>
          chartVar.now().foreach(_.destroy())
          chartVar.set(None)
        }
      )
    )
  }

  def main(args: Array[String]): Unit =
    render(dom.document.body, app): Unit
}
