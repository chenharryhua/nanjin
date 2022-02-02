package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.kernel.{Async, Sync}
import cats.syntax.functor.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJParquet, NJPath}
import com.sksamuel.avro4s.Decoder
import frameless.TypedEncoder
import fs2.Stream
import io.circe.Decoder as JsonDecoder
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.SparkSession

final class LoadTopicFile[F[_], K, V] private[kafka] (topic: KafkaTopic[F, K, V], cfg: SKConfig, ss: SparkSession)
    extends Serializable {

  private val ack: NJAvroCodec[K] = topic.topicDef.rawSerdes.keySerde.avroCodec
  private val acv: NJAvroCodec[V] = topic.topicDef.rawSerdes.valSerde.avroCodec

  def avro(path: NJPath)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.avro[NJConsumerRecord[K, V]](path, ate, ss)
      new CrDS(tds, cfg, ack, acv, tek, tev)
    }

  def parquet(path: NJPath)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.parquet[NJConsumerRecord[K, V]](path, ate, ss)
      new CrDS(tds, cfg, ack, acv, tek, tev)
    }

  def json(path: NJPath)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.json[NJConsumerRecord[K, V]](path, ate, ss)
      new CrDS(tds, cfg, ack, acv, tek, tev)
    }

  def jackson(path: NJPath)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.jackson[NJConsumerRecord[K, V]](path, ate, ss)
      new CrDS(tds, cfg, ack, acv, tek, tev)
    }

  def binAvro(path: NJPath)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.binAvro[NJConsumerRecord[K, V]](path, ate, ss)
      new CrDS(tds, cfg, ack, acv, tek, tev)
    }

  def circe(path: NJPath)(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V],
    jdk: JsonDecoder[K],
    jdv: JsonDecoder[V],
    F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.circe[NJConsumerRecord[K, V]](path, ate, ss)
      new CrDS(tds, cfg, ack, acv, tek, tev)
    }

  def objectFile(path: NJPath)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.objectFile[NJConsumerRecord[K, V]](path, ate, ss)
      new CrDS(tds, cfg, ack, acv, tek, tev)
    }

  private val decoder: Decoder[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(topic.topicDef).avroDecoder

  object rdd {

    def avro(path: NJPath)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.avro[NJConsumerRecord[K, V]](path, decoder, ss)
        new CrRdd[F, K, V](rdd, ack, acv, cfg, ss)
      }

    def jackson(path: NJPath)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.jackson[NJConsumerRecord[K, V]](path, decoder, ss)
        new CrRdd[F, K, V](rdd, ack, acv, cfg, ss)
      }

    def binAvro(path: NJPath)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.binAvro[NJConsumerRecord[K, V]](path, decoder, ss)
        new CrRdd[F, K, V](rdd, ack, acv, cfg, ss)
      }

    def circe(path: NJPath)(implicit ev: JsonDecoder[NJConsumerRecord[K, V]], F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.circe[NJConsumerRecord[K, V]](path, ss)
        new CrRdd[F, K, V](rdd, ack, acv, cfg, ss)
      }

    def objectFile(path: NJPath)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.objectFile[NJConsumerRecord[K, V]](path, ss)
        new CrRdd[F, K, V](rdd, ack, acv, cfg, ss)
      }
  }

  object stream {
    private val hadoopConfiguration: Configuration = ss.sparkContext.hadoopConfiguration

    def circe(
      path: NJPath)(implicit F: Sync[F], jdk: JsonDecoder[K], jdv: JsonDecoder[V]): Stream[F, NJConsumerRecord[K, V]] =
      loaders.stream.circe[F, NJConsumerRecord[K, V]](path, hadoopConfiguration)

    def jackson(path: NJPath)(implicit F: Async[F]): Stream[F, NJConsumerRecord[K, V]] =
      loaders.stream.jackson[F, NJConsumerRecord[K, V]](path, decoder, hadoopConfiguration)

    def avro(path: NJPath, chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, NJConsumerRecord[K, V]] =
      loaders.stream.avro[F, NJConsumerRecord[K, V]](path, decoder, hadoopConfiguration, chunkSize)

    def parquet(path: NJPath)(implicit F: Sync[F]): Stream[F, NJConsumerRecord[K, V]] =
      Stream.force(
        NJHadoop[F](hadoopConfiguration)
          .locatedFileStatus(path)
          .map(_.filter(_.isFile)
            .foldLeft(Stream.empty.covaryAll[F, GenericRecord]) { case (stm, lfs) =>
              val builder: AvroParquetReader.Builder[GenericRecord] =
                AvroParquetReader
                  .builder[GenericRecord](HadoopInputFile.fromPath(lfs.getPath, hadoopConfiguration))
                  .withDataModel(GenericData.get())
              stm ++ NJParquet.fs2Source[F](builder).handleErrorWith(_ => Stream.empty)
            }
            .map(decoder.decode)))
  }
}
