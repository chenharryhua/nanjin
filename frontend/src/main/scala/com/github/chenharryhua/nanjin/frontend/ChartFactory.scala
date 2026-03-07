package com.github.chenharryhua.nanjin.frontend

import org.scalajs.dom.HTMLCanvasElement

import scala.scalajs.js
import scala.scalajs.js.Dynamic.literal

object ChartFactory {
  def lines(canvas: HTMLCanvasElement, zoneId: String): js.Dynamic = {
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
          cubicInterpolationMode = "monotone",
          maintainAspectRatio = false,
          spanGaps = false,
          scales = literal(
            x = literal(
              `type` = "time",
              time = literal(unit = "minute", tooltipFormat = "HH:mm"),
              title = literal(display = true, text = "Time"),
              adapters = literal(date = literal(zone = zoneId))
            ),
            y = literal(
              beginAtZero = true,
              title = literal(display = true, text = "Value"),
              ticks = literal(
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

}
