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
  private val dashboard: ReactiveHtmlElement[HTMLDivElement] =
    div(
      width  := "98%",
      height := "90vh",
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
    val _ = render(dom.document.body, dashboard)
  }
}
