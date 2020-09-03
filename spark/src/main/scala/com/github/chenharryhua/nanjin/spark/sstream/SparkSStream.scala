package com.github.chenharryhua.nanjin.spark.sstream
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

trait SparkStreamUpdateParams[A] extends UpdateParams[SStreamConfig, A] with Serializable {
  def params: SStreamParams
}

final class SparkSStream[F[_], A: TypedEncoder](ds: Dataset[A], cfg: SStreamConfig)
    extends SparkStreamUpdateParams[SparkSStream[F, A]] {

  override val params: SStreamParams = cfg.evalConfig

  override def withParamUpdate(f: SStreamConfig => SStreamConfig): SparkSStream[F, A] =
    new SparkSStream[F, A](ds, f(cfg))

  @transient lazy val typedDataset: TypedDataset[A] = TypedDataset.create(ds)

  // transforms

  def filter(f: A => Boolean): SparkSStream[F, A] =
    new SparkSStream[F, A](ds.filter(f), cfg)

  def map[B: TypedEncoder](f: A => B): SparkSStream[F, B] =
    new SparkSStream[F, B](typedDataset.deserialized.map(f).dataset, cfg)

  def flatMap[B: TypedEncoder](f: A => TraversableOnce[B]): SparkSStream[F, B] =
    new SparkSStream[F, B](typedDataset.deserialized.flatMap(f).dataset, cfg)

  def transform[B: TypedEncoder](f: TypedDataset[A] => TypedDataset[B]) =
    new SparkSStream[F, B](f(typedDataset).dataset, cfg)

  // sinks

  def consoleSink: NJConsoleSink[F, A] =
    new NJConsoleSink[F, A](ds.writeStream, cfg)

  def fileSink(path: String): NJFileSink[F, A] =
    new NJFileSink[F, A](ds.writeStream, cfg, path)

  def kafkaSink[K: TypedEncoder, V: TypedEncoder](kit: KafkaTopic[F, K, V])(implicit
    ev: A =:= NJProducerRecord[K, V]): NJKafkaSink[F] =
    new KafkaPrSStream[F, K, V](typedDataset.deserialized.map(ev).dataset, cfg).kafkaSink(kit)
}
