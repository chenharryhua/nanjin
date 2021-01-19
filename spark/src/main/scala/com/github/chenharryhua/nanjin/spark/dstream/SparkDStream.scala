package com.github.chenharryhua.nanjin.spark.dstream

import org.apache.spark.streaming.dstream.DStream

final class SparkDStream[F[_], A](val dstream: DStream[A]) {}
