package com.github.chenharryhua.nanjin.spark.dstream

import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.sksamuel.avro4s.Encoder as AvroEncoder
import io.circe.Encoder as JsonEncoder
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

final class DStreamSink[A](dstream: DStream[A], cfg: SDConfig)
    extends UpdateConfig[SDConfig, DStreamSink[A]] with Serializable {
  val params: SDParams = cfg.evalConfig

  override def updateConfig(f: SDConfig => SDConfig): DStreamSink[A] =
    new DStreamSink[A](dstream, f(cfg))

  def pathBuilder(f: String => NJTimestamp => String): DStreamSink[A] = updateConfig(_.pathBuilder(f))

  def transform[B](f: DStream[A] => DStream[B]): DStreamSink[B] = new DStreamSink(f(dstream), cfg)

  def coalesce(implicit tag: ClassTag[A]): DStreamSink[A] = transform(_.transform(_.coalesce(1)))

  def circe(path: String)(implicit enc: JsonEncoder[A]): DStreamRunner.Mark =
    persist.circe[A](dstream)(params.pathBuilder(path))

}

final class AvroDStreamSink[A](dstream: DStream[A], encoder: AvroEncoder[A], cfg: SDConfig) extends Serializable {
  val params: SDParams = cfg.evalConfig

  private def updateConfig(f: SDConfig => SDConfig): AvroDStreamSink[A] =
    new AvroDStreamSink[A](dstream, encoder, f(cfg))

  def pathBuilder(f: String => NJTimestamp => String): AvroDStreamSink[A] = updateConfig(_.pathBuilder(f))

  def transform[B](f: DStream[A] => DStream[B], encoder: AvroEncoder[B]): AvroDStreamSink[B] =
    new AvroDStreamSink(f(dstream), encoder, cfg)

  def coalesce(implicit tag: ClassTag[A]): AvroDStreamSink[A] = transform(_.transform(_.coalesce(1)), encoder)

  def circe(path: String)(implicit enc: JsonEncoder[A]): DStreamRunner.Mark =
    persist.circe[A](dstream)(params.pathBuilder(path))

  def avro(path: String): DStreamRunner.Mark =
    persist.avro[A](dstream, encoder)(params.pathBuilder(path))

  def jackson(path: String): DStreamRunner.Mark =
    persist.jackson[A](dstream, encoder)(params.pathBuilder(path))
}
