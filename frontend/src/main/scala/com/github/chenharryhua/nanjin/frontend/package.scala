package com.github.chenharryhua.nanjin
import scala.collection.mutable
import scala.scalajs.js

package object frontend {

  private val colorMap: mutable.Map[String, String] = mutable.Map.empty
  private def colorFor(name: String): String =
    colorMap.getOrElseUpdate(name, s"hsl(${math.abs(name.hashCode % 360)}, 70%, 50%)")

  def appendToChart(chart: js.Dynamic, msg: WsMessage, maxPoints: Int = 100): Unit = {
    val existingDatasets = chart.data.datasets.asInstanceOf[js.Array[js.Dynamic]]

    msg.series.foreach { s =>
      val newPoint = s.point.dataPoint
      existingDatasets.find(d => d.label.toString == s.name) match {
        case Some(ds) =>
          val dsData = ds.data.asInstanceOf[js.Array[js.Dynamic]]
          dsData.push(newPoint):Unit
          if (dsData.length > maxPoints) dsData.shift() // sliding window
        case None =>
          val borderColor = colorFor(s.name)
          existingDatasets.push(s.toDataset(borderColor))
      }
    }

    chart.update():Unit
  }
}
