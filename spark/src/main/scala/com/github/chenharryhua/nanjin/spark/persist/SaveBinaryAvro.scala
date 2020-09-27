package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.pipes.{BinaryAvroSerialization, GenericRecordCodec}
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveBinaryAvro[F[_], A](rdd: RDD[A], codec: AvroCodec[A], cfg: HoarderConfig)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  def run(blocker: Blocker)(implicit
    F: Concurrent[F],
    cs: ContextShift[F],
    ss: SparkSession,
    tag: ClassTag[A]): F[Unit] = {
    implicit val encoder: Encoder[A] = codec.avroEncoder

    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)
    val hadoop =
      new NJHadoop[F](ss.sparkContext.hadoopConfiguration).byteSink(params.outPath, blocker)
    val gr   = new GenericRecordCodec[F, A]
    val pipe = new BinaryAvroSerialization[F](codec.schema)

    sma.checkAndRun(blocker)(
      rdd.stream[F].through(gr.encode).through(pipe.serialize).through(hadoop).compile.drain)
  }
}
