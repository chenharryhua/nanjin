package com.github.chenharryhua.nanjin.sparkafka

import frameless.TypedDataset

trait Aggregations {
  implicit class PredefinedAggregationFunction(tds: TypedDataset[SparkConsumerRecord[_, _]]) {}
}
