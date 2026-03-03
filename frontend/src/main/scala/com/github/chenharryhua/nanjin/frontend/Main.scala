package com.github.chenharryhua.nanjin.frontend
import com.raquo.laminar.api.L._
import com.raquo.laminar.nodes.ReactiveHtmlElement
import io.circe.jawn.decode
import org.scalajs.dom
import org.scalajs.dom.{CloseEvent, Event, HTMLCanvasElement, HTMLDivElement, MessageEvent, WebSocket}

import scala.scalajs.js
import scala.scalajs.js.Dynamic.literal

object Main {

  /*
   * from backend
   */
  private val config: BackendConfig = BackendConfig.load()

  /*
   * Chart
   */
  private val chartVar: Var[Option[js.Dynamic]] = Var(Option.empty[js.Dynamic])
  private val manager: ChartManager = new ChartManager(config.maxPoints)

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

  private def connectWS(port: Int): Unit = {

    val ws = new WebSocket(s"ws://localhost:$port/ws")

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

          val chart = initChart(canvas, config.zoneId)
          chartVar.set(Some(chart))

          connectWS(config.port)
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
  def main(args: Array[String]): Unit = {
    val _ = render(dom.document.body, app)
  }
}
