package com.github.chenharryhua.nanjin.spark.dstream

import cats.data.Reader
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.spark.persist.saveRDD
import com.github.chenharryhua.nanjin.terminals.{NJCompression, NJPath}
import com.sksamuel.avro4s.Encoder as AvroEncoder
import io.circe.Encoder as JsonEncoder
import org.apache.spark.streaming.dstream.DStream

private[dstream] object persist {

  def circe[A: JsonEncoder](
    ds: DStream[A],
    pathBuilder: Reader[NJTimestamp, NJPath],
    isKeepNull: Boolean): DStreamRunner.Mark = {
    ds.foreachRDD { (rdd, time) =>
      val path: NJPath = pathBuilder.run(NJTimestamp(time.milliseconds))
      saveRDD.circe[A](rdd, path, NJCompression.Uncompressed, isKeepNull)
    }
    DStreamRunner.Mark
  }

  def jackson[A](
    ds: DStream[A],
    encoder: AvroEncoder[A],
    pathBuilder: Reader[NJTimestamp, NJPath]): DStreamRunner.Mark = {
    ds.foreachRDD { (rdd, time) =>
      val path: NJPath = pathBuilder.run(NJTimestamp(time.milliseconds))
      saveRDD.jackson[A](rdd, path, encoder, NJCompression.Uncompressed)
    }
    DStreamRunner.Mark
  }

  def avro[A](
    ds: DStream[A],
    encoder: AvroEncoder[A],
    pathBuilder: Reader[NJTimestamp, NJPath]): DStreamRunner.Mark = {
    ds.foreachRDD { (rdd, time) =>
      val path: NJPath = pathBuilder.run(NJTimestamp(time.milliseconds))
      saveRDD.avro[A](rdd, path, encoder, NJCompression.Uncompressed)
    }
    DStreamRunner.Mark
  }
}
