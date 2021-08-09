package com.github.chenharryhua.nanjin.spark.dstream

import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import com.github.chenharryhua.nanjin.spark.persist.{saveRDD, Compression}
import com.sksamuel.avro4s.Encoder as AvroEncoder
import io.circe.Encoder as JsonEncoder
import org.apache.spark.streaming.dstream.DStream

private[dstream] object persist {

  def circe[A: JsonEncoder](ds: DStream[A])(pathBuilder: NJTimestamp => String): DStreamRunner.Mark = {
    ds.foreachRDD { (rdd, time) =>
      val path = pathBuilder(NJTimestamp(time.milliseconds))
      saveRDD.circe(rdd, path, Compression.Uncompressed, isKeepNull = true)
    }
    DStreamRunner.Mark
  }

  def jackson[A](ds: DStream[A], encoder: AvroEncoder[A])(pathBuilder: NJTimestamp => String): DStreamRunner.Mark = {
    ds.foreachRDD { (rdd, time) =>
      val path = pathBuilder(NJTimestamp(time.milliseconds))
      saveRDD.jackson(rdd, path, encoder, Compression.Uncompressed)
    }
    DStreamRunner.Mark
  }

  def avro[A](ds: DStream[A], encoder: AvroEncoder[A])(pathBuilder: NJTimestamp => String): DStreamRunner.Mark = {
    ds.foreachRDD { (rdd, time) =>
      val path = pathBuilder(NJTimestamp(time.milliseconds))
      saveRDD.avro(rdd, path, encoder, Compression.Snappy)
    }
    DStreamRunner.Mark
  }
}
