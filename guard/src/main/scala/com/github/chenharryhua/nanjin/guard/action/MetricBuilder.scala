package com.github.chenharryhua.nanjin.guard.action

import com.github.chenharryhua.nanjin.common.EnableConfig

trait MetricBuilder[A] extends EnableConfig[A] {
  def withTag(tag: String): A
  def withMeasurement(measurement: String): A
}
