package com.github.chenharryhua.nanjin.frontend

import com.raquo.laminar.api.L.Var

import scala.collection.mutable
import scala.scalajs.js
import scala.scalajs.js.JSConverters.JSRichIterableOnce

/*
 * Mutable world
 */
final class ChartManager(maxSizePerSeries: Int) {

  private val colorMap: mutable.Map[String, String] = mutable.Map.empty
  private def colorFor(name: String): String =
    colorMap.getOrElseUpdate(name, s"hsl(${math.abs(name.hashCode % 360)}, 70%, 50%)")

  private val data: mutable.Map[String, mutable.Queue[Point]] = mutable.Map.empty

  def enqueue(msg: WsMessage): ChartManager = {
    // All series we need to update: existing + new
    val allNames = data.keys.toSet ++ msg.points.keys.toSet

    allNames.foreach { name =>
      val queue = data.getOrElseUpdate(name, mutable.Queue.empty)

      if (queue.size >= maxSizePerSeries) queue.dequeue(): Unit

      // Enqueue new point if available, else a placeholder for fading
      val newPoint = msg.points.getOrElse(name, Point(msg.ts, None))
      queue.enqueue(newPoint)
    }

    // chart get shorter and shorter
    // data.filterInPlace { case (_, q) => q.exists(_.y.isDefined) }

    this
  }

  def updateChart(chartVar: Var[Option[js.Dynamic]]): Unit =
    chartVar.now().foreach { chart =>
      val datasets = chart.data.datasets.asInstanceOf[js.Array[js.Dynamic]]

      // remove fade out series
      val toRemove = data.collect { case (k, q) if q.forall(_.y.isEmpty) => k }
      toRemove
        .foreach { label =>
          val i = datasets.indexWhere(_.label.asInstanceOf[String] == label)
          if (i >= 0) datasets.splice(i, 1)
        }
      toRemove.foreach(data.remove)

      // build up datasets
      datasets.foreach { dataset =>
        val name = dataset.label.asInstanceOf[String]
        data.get(name).foreach { queue =>
          dataset.data = queue.iterator.map(_.dataPoint).toJSArray
        }
      }

      // Add new series if they don’t exist yet
      data.keys.foreach { name =>
        if (!datasets.exists(_.label.asInstanceOf[String] == name)) {
          val queue = data(name)
          val newDataset = js.Dynamic.literal(
            label = name,
            data = queue.iterator.map(_.dataPoint).toJSArray,
            borderColor = colorFor(name),
            backgroundColor = colorFor(name),
            fill = false,
            tension = 0.3,
            pointRadius = 0
          )
          datasets.push(newDataset)
        }
      }

      chart.update()
    }
}
