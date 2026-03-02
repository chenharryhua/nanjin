package com.github.chenharryhua.nanjin.frontend
import com.raquo.laminar.api.L._
import com.raquo.laminar.nodes.ReactiveHtmlElement
import io.circe.jawn.decode
import org.scalajs.dom
import org.scalajs.dom.{document, html, HTMLCanvasElement, HTMLDivElement, MessageEvent, WebSocket}

import java.time.{Instant, LocalDateTime, ZoneId}
import scala.scalajs.js
import scala.scalajs.js.Dynamic.literal

object Main {

  /*
   * from backend
   */
  private val rootDiv = document.getElementById("chart-root").asInstanceOf[html.Div]
  private val port: String = rootDiv.dataset("ws_port")
  private val zoneId: String = rootDiv.dataset("zone_id")
  private val maxPoints: Int = rootDiv.dataset("max_points").toInt

  /*
   * Chart
   */
  private val chartVar: Var[Option[js.Dynamic]] = Var(Option.empty[js.Dynamic])
  private val manager: ChartManager = new ChartManager(maxPoints)

  /*
   * Initialization
   */
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
                  val abs = Math.abs(v)

                  def fmt(x: Double, unit: String): String = {
                    val s = f"$x%.1f"
                    if (s.endsWith(".0")) s.dropRight(2) + unit
                    else s + unit
                  }

                  if (abs >= 1_000_000) fmt(v / 1_000_000, "M")
                  else if (abs >= 1_000) fmt(v / 1_000, "k")
                  else Math.round(v).toString
                }
              )
            )
          ),
          plugins = literal(legend = literal(display = true))
        )
      )
    )
  }

  /*
   * Websocket
   */
  private def datetime(ts: Double): LocalDateTime =
    Instant.ofEpochMilli(ts.toLong).atZone(ZoneId.of(zoneId)).toLocalDateTime

  private def connectWS(port: String): Unit = {

    val ws = new WebSocket(s"ws://localhost:$port/ws")

    ws.onopen = { o =>
      val now = datetime(o.timeStamp).toString
      dom.console.log(s"WS connected at $now")
    }

    ws.onmessage = { (e: MessageEvent) =>
      ws.send("pong")
      decode[WsMessage](e.data.toString).toOption.foreach { msg =>
        manager.enqueue(msg).updateChart(chartVar)
      }
    }

    ws.onclose = { c =>
      val now = datetime(c.timeStamp).toString
      val cause = s"reason:${c.reason}, code:${c.code}, wasClean:${c.wasClean}"
      dom.console.log(s"WS closed at $now. $cause")
    }
  }

  /*
   * Canvas
   */
  private val app: ReactiveHtmlElement[HTMLDivElement] =
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

  /*
   * Start from here
   */
  def main(args: Array[String]): Unit =
    render(dom.document.body, app): Unit
}
