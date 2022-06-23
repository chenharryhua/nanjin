package com.github.chenharryhua.nanjin.spark.dstream

import cats.data.Reader
import com.github.chenharryhua.nanjin.spark.dstream.DStreamRunner.Mark
import com.github.chenharryhua.nanjin.spark.SPARK_ZONE_ID
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.Encoder as AvroEncoder
import io.circe.Encoder as JsonEncoder
import org.apache.spark.streaming.dstream.DStream

import java.time.{LocalDateTime, ZoneId}
import scala.reflect.ClassTag

final class DStreamSink[A](dstream: DStream[A], pathBuilder: NJPath => LocalDateTime => NJPath)
    extends Serializable {
  val zoneId: ZoneId = ZoneId.of(dstream.context.sparkContext.getConf.get(SPARK_ZONE_ID))

  def transform[B](f: DStream[A] => DStream[B]): DStreamSink[B] = new DStreamSink(f(dstream), pathBuilder)

  def coalesce(implicit tag: ClassTag[A]): DStreamSink[A] = transform(_.transform(_.coalesce(1)))

  def circe(root: NJPath)(implicit encoder: JsonEncoder[A]): Mark =
    persist.circe(dstream, zoneId, encoder, Reader(pathBuilder(root)))

}

final class AvroDStreamSink[A](
  dstream: DStream[A],
  encoder: AvroEncoder[A],
  pathBuilder: NJPath => LocalDateTime => NJPath)
    extends Serializable {
  val zoneId: ZoneId = ZoneId.of(dstream.context.sparkContext.getConf.get(SPARK_ZONE_ID))

  def withPathBuilder(f: NJPath => LocalDateTime => NJPath): AvroDStreamSink[A] =
    new AvroDStreamSink[A](dstream, encoder, f)

  def transform[B](encoder: AvroEncoder[B])(f: DStream[A] => DStream[B]): AvroDStreamSink[B] =
    new AvroDStreamSink(f(dstream), encoder, pathBuilder)

  def coalesce(implicit tag: ClassTag[A]): AvroDStreamSink[A] = transform(encoder)(_.transform(_.coalesce(1)))

  def circe(root: NJPath)(implicit encoder: JsonEncoder[A]): Mark =
    persist.circe(dstream, zoneId, encoder, Reader(pathBuilder(root)))
  def avro(root: NJPath): Mark    = persist.avro(dstream, zoneId, encoder, Reader(pathBuilder(root)))
  def jackson(root: NJPath): Mark = persist.jackson(dstream, zoneId, encoder, Reader(pathBuilder(root)))
}
