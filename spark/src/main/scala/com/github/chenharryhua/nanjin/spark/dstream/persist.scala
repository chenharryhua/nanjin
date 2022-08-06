package com.github.chenharryhua.nanjin.spark.dstream

import cats.data.Reader
import com.github.chenharryhua.nanjin.spark.persist.saveRDD
import com.github.chenharryhua.nanjin.terminals.{NJCompression, NJPath}
import com.sksamuel.avro4s.Encoder as AvroEncoder
import io.circe.Encoder as JsonEncoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Time

import java.time.{Instant, LocalDateTime, ZoneId}

private[dstream] object persist {
  private def getPath(builder: Reader[LocalDateTime, NJPath], time: Time, zoneId: ZoneId): NJPath =
    builder.run(Instant.ofEpochMilli(time.milliseconds).atZone(zoneId).toLocalDateTime)

  def circe[A](
    ds: DStream[A],
    zoneId: ZoneId,
    encoder: JsonEncoder[A],
    pathBuilder: Reader[LocalDateTime, NJPath]): DStreamRunner.Mark = {
    ds.foreachRDD { (rdd, time) =>
      val path: NJPath = getPath(pathBuilder, time, zoneId)
      saveRDD.circe[A](rdd, path, NJCompression.Uncompressed, isKeepNull = true)(encoder)
    }
    DStreamRunner.Mark
  }

  def jackson[A](
    ds: DStream[A],
    zoneId: ZoneId,
    encoder: AvroEncoder[A],
    pathBuilder: Reader[LocalDateTime, NJPath]): DStreamRunner.Mark = {
    ds.foreachRDD { (rdd, time) =>
      val path: NJPath = getPath(pathBuilder, time, zoneId)
      saveRDD.jackson[A](rdd, path, encoder, NJCompression.Uncompressed)
    }
    DStreamRunner.Mark
  }

  def avro[A](
    ds: DStream[A],
    zoneId: ZoneId,
    encoder: AvroEncoder[A],
    pathBuilder: Reader[LocalDateTime, NJPath]): DStreamRunner.Mark = {
    ds.foreachRDD { (rdd, time) =>
      val path: NJPath = getPath(pathBuilder, time, zoneId)
      saveRDD.avro[A](rdd, path, encoder, NJCompression.Uncompressed)
    }
    DStreamRunner.Mark
  }
}
