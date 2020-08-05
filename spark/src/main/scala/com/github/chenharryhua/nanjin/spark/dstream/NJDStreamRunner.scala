package com.github.chenharryhua.nanjin.spark.dstream

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}

final class NJDStreamRunner(sparkSession: SparkSession, checkpoint: String, duration: Duration) {
  val ctx = new StreamingContext(sparkSession.sparkContext, duration)
  ctx.checkpoint(checkpoint)

  ctx.start()
  ctx.stop(false, true)
  ctx.awaitTermination()
}
