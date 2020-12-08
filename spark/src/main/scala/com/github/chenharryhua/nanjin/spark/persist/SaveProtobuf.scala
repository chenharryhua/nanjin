package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.pipes.DelimitedProtoBufSerialization
import com.github.chenharryhua.nanjin.spark.RddExt
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scalapb.GeneratedMessage

import java.io.ByteArrayOutputStream
import scala.reflect.ClassTag

final class SaveProtobuf[F[_], A](rdd: RDD[A], codec: AvroCodec[A], cfg: HoarderConfig)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveProtobuf[F, A] =
    new SaveProtobuf[F, A](rdd, codec, cfg)

  def file: SaveProtobuf[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveProtobuf[F, A] = updateConfig(cfg.withFolder)

  def run(blocker: Blocker)(implicit
    F: Concurrent[F],
    cs: ContextShift[F],
    ss: SparkSession,
    enc: A <:< GeneratedMessage,
    tag: ClassTag[A]): F[Unit] = {

    def bytesWritable(a: A): BytesWritable = {
      val os: ByteArrayOutputStream = new ByteArrayOutputStream()
      enc(a).writeDelimitedTo(os)
      os.close()
      new BytesWritable(os.toByteArray)
    }

    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)
    val ccg: CompressionCodecGroup[F] =
      params.compression.ccg[F](ss.sparkContext.hadoopConfiguration)

    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        val hadoop: NJHadoop[F]                     = NJHadoop[F](ss.sparkContext.hadoopConfiguration, blocker)
        val pipe: DelimitedProtoBufSerialization[F] = new DelimitedProtoBufSerialization[F]

        sma.checkAndRun(blocker)(
          rdd
            .map(codec.idConversion)
            .stream[F]
            .through(pipe.serialize(blocker))
            .through(hadoop.byteSink(params.outPath))
            .compile
            .drain)

      case FolderOrFile.Folder =>
        ss.sparkContext.hadoopConfiguration.set(NJBinaryOutputFormat.suffix, params.format.suffix)
        sma.checkAndRun(blocker)(
          F.delay(
            rdd
              .map(x => (NullWritable.get(), bytesWritable(x)))
              .saveAsNewAPIHadoopFile[NJBinaryOutputFormat](params.outPath)))

    }
  }
}
