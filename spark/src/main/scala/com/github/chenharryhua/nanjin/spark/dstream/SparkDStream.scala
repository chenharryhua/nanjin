package com.github.chenharryhua.nanjin.spark.dstream

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import io.circe.{Encoder => JsonEncoder}
import monocle.{Getter, Setter}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

final class SparkDStream[A](val dstream: DStream[A], cfg: SDConfig) extends Serializable {
  val params: SDParams = cfg.evalConfig

  private def updateConfig(f: SDConfig => SDConfig): SparkDStream[A] =
    new SparkDStream[A](dstream, f(cfg))

  def pathBuilder(f: String => NJTimestamp => String): SparkDStream[A] = updateConfig(_.withPathBuilder(f))

}

object SparkDStream {

  implicit def getterSparkDStream[A]: Getter[SparkDStream[A], DStream[A]] =
    (s: SparkDStream[A]) => s.dstream
}

final class SparkAvroDStream[A](val dstream: DStream[A], encoder: AvroEncoder[A], cfg: SDConfig) extends Serializable {
  val params: SDParams = cfg.evalConfig

  private def updateConfig(f: SDConfig => SDConfig): SparkAvroDStream[A] =
    new SparkAvroDStream[A](dstream, encoder, f(cfg))

  def pathBuilder(f: String => NJTimestamp => String): SparkAvroDStream[A] = updateConfig(_.withPathBuilder(f))

  private def checkpoint(fd: FiniteDuration): SparkAvroDStream[A] =
    updateConfig(_.withCheckpointDuration(Seconds(fd.toSeconds)))

  def circe(path: String)(implicit enc: JsonEncoder[A]): EndMark =
    persist.circe[A](dstream)(params.pathBuilder(path))

  def avro(path: String): EndMark =
    persist.avro(dstream, encoder)(params.pathBuilder(path))

  def jackson(path: String): EndMark =
    persist.jackson(dstream.checkpoint(params.checkpointDuration), encoder)(params.pathBuilder(path))
}

object SparkAvroDStream {

  implicit def setterSparkAvroDStream[A]: Setter[SparkAvroDStream[A], FiniteDuration] =
    new Setter[SparkAvroDStream[A], FiniteDuration] {

      override def set(b: FiniteDuration): SparkAvroDStream[A] => SparkAvroDStream[A] = _.checkpoint(b)

      override def modify(f: FiniteDuration => FiniteDuration): SparkAvroDStream[A] => SparkAvroDStream[A] =
        (sa: SparkAvroDStream[A]) =>
          sa.checkpoint(f(FiniteDuration.apply(sa.params.checkpointDuration.milliseconds, TimeUnit.MILLISECONDS)))
    }

  implicit def getterSparkAvroDStream[A]: Getter[SparkAvroDStream[A], DStream[A]] =
    (s: SparkAvroDStream[A]) => s.dstream
}
