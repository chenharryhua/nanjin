package com.github.chenharryhua.nanjin.spark.dstream

import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import com.github.chenharryhua.nanjin.spark.persist.{saveRDD, NJCompression}
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.Encoder as AvroEncoder
import io.circe.Encoder as JsonEncoder
import org.apache.spark.streaming.dstream.DStream

private[dstream] object persist {

  def circe[A: JsonEncoder](ds: DStream[A])(pathBuilder: NJTimestamp => NJPath): DStreamRunner.Mark = {
    ds.foreachRDD { (rdd, time) =>
      val path = pathBuilder(NJTimestamp(time.milliseconds))
      saveRDD.circe(rdd, path, NJCompression.Uncompressed, isKeepNull = true)
    }
    DStreamRunner.Mark
  }

  def jackson[A](ds: DStream[A], encoder: AvroEncoder[A])(pathBuilder: NJTimestamp => NJPath): DStreamRunner.Mark = {
    ds.foreachRDD { (rdd, time) =>
      val path = pathBuilder(NJTimestamp(time.milliseconds))
      saveRDD.jackson(rdd, path, encoder, NJCompression.Uncompressed)
    }
    DStreamRunner.Mark
  }

  def avro[A](ds: DStream[A], encoder: AvroEncoder[A])(pathBuilder: NJTimestamp => NJPath): DStreamRunner.Mark = {
    ds.foreachRDD { (rdd, time) =>
      val path = pathBuilder(NJTimestamp(time.milliseconds))
      saveRDD.avro(rdd, path, encoder, NJCompression.Snappy)
    }
    DStreamRunner.Mark
  }
}
