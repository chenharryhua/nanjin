package com.github.chenharryhua.nanjin.frontend
import com.raquo.laminar.api.L._
import com.raquo.laminar.nodes.ReactiveHtmlElement
import org.scalajs.dom
import org.scalajs.dom.HTMLDivElement

import scala.scalajs.js

object Main {

  /*
   * from backend
   */
  private val config: BackendConfig = BackendConfig.load()

  /*
   * Chart
   */
  private val chartVar: Var[Option[js.Dynamic]] = Var(Option.empty[js.Dynamic])
  private val connector: WsConnector =
    new WsConnector(FrontendConfig.fromWindow(), config.maxPoints)

  /*
   * Canvas
   */
  private val banner: ReactiveHtmlElement[HTMLDivElement] =
    div(
      display.flex,
      justifyContent.center,
      alignItems.center,
      padding        := "8px",
      backgroundColor := "#e74c3c",
      color          := "white",
      fontWeight.bold,
      fontSize       := "14px",
      display <-- connector.connected.signal.map(if (_) "none" else "flex"),
      "Disconnected — reconnecting..."
    )

  private val staleBanner: ReactiveHtmlElement[HTMLDivElement] =
    div(
      display.flex,
      justifyContent.center,
      alignItems.center,
      padding        := "8px",
      backgroundColor := "#f39c12",
      color          := "white",
      fontWeight.bold,
      fontSize       := "14px",
      display <-- connector.stale.signal.map(if (_) "flex" else "none"),
      "Data stale — no updates received"
    )

  private val dashboard: ReactiveHtmlElement[HTMLDivElement] =
    div(
      width  := "98%",
      height := "90vh",
      banner,
      staleBanner,
      h2(
        s"Service: ${config.serviceName}",
        title := s"maxPoints=${config.maxPoints}, policy=${config.policy}"
      ),
      canvasTag(
        width  := "100%",
        height := "100%",

        onMountCallback { ctx =>
          val chart = ChartFactory.lines(ctx.thisNode.ref, config.zoneId)
          chartVar.set(Some(chart))

          connector.connect(chartVar)
        },

        // cleanup
        onUnmountCallback { _ =>
          connector.close()
          chartVar.now().foreach(_.destroy())
          chartVar.set(None)
        }
      )
    )

  /*
   * Start from here
   */
  def main(args: Array[String]): Unit = {
    val _ = render(dom.document.body, dashboard)
  }
}
