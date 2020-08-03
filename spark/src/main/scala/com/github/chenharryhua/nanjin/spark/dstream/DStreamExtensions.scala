package com.github.chenharryhua.nanjin.spark.dstream

import org.apache.spark.streaming.dstream.DStream

private[dstream] trait DStreamExtensions {

  implicit class DStreamExt[A](val ds: DStream[A]) {
    def avro = ds.foreachRDD((r, t) => ())
  }

}
