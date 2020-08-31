package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.{fileSink, utils, RddExt}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class RddFileSaver[F[_], A: ClassTag](rdd: RDD[A], cfg: SaverConfig = SaverConfig())(implicit
  codec: NJAvroCodec[A],
  ss: SparkSession)
    extends Serializable {

  private def updateConfig(cfg: SaverConfig): RddFileSaver[F, A] =
    new RddFileSaver[F, A](rdd, cfg)

  def overwrite: RddFileSaver[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: RddFileSaver[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: RddFileSaver[F, A] = updateConfig(cfg.withIgnore)

  def single: RddFileSaver[F, A] = updateConfig(cfg.withSingle)
  def multi: RddFileSaver[F, A]  = updateConfig(cfg.withMulti)

  def spark: RddFileSaver[F, A] = updateConfig(cfg.withSpark)
  def raw: RddFileSaver[F, A]   = updateConfig(cfg.withRaw)

  val params: SaverParams = cfg.evalConfig

  private def sma = new SaveModeAware[F](params.saveMode, ss)

  def avro(outPath: String): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, outPath, sma, cfg.evalConfig)

  def parquet(outPath: String): SaveParquet[F, A] =
    new SaveParquet[F, A](rdd, outPath, sma, cfg.evalConfig)

  def jackson(outPath: String): SaveJackson[F, A] =
    new SaveJackson[F, A](rdd, outPath, sma, cfg.evalConfig)

  def binAvro(outPath: String): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, outPath, sma, cfg.evalConfig)

  def circe(outPath: String)(implicit ev: JsonEncoder[A]): SaveCirce[F, A] =
    new SaveCirce[F, A](rdd, outPath, sma, cfg.evalConfig)

  def csv(outPath: String)(implicit ev: RowEncoder[A]): SaveCSV[F, A] =
    new SaveCSV[F, A](rdd, outPath, CsvConfiguration.rfc, sma, params)

  def text(outPath: String)(implicit ev: Show[A]): SaveText[F, A] =
    new SaveText[F, A](rdd, outPath, sma, params)

}

final class SaveAvro[F[_], A: ClassTag](
  rdd: RDD[A],
  outPath: String,
  sma: SaveModeAware[F],
  params: SaverParams)(implicit codec: NJAvroCodec[A], ss: SparkSession)
    extends Serializable {

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    implicit val encoder: AvroEncoder[A] = codec.avroEncoder
    (params.singleOrMulti, params.sparkOrRaw) match {
      case (SingleOrMulti.Single, _) =>
        sma.run(
          rdd.stream[F].through(fileSink[F](blocker).avro(outPath)).compile.drain,
          outPath,
          blocker)
      case (SingleOrMulti.Multi, SparkOrRaw.Spark) =>
        sma.run(F.delay(savers.avro(rdd, outPath)), outPath, blocker)
      case (SingleOrMulti.Multi, SparkOrRaw.Raw) =>
        sma.run(F.delay(savers.raw.avro(rdd, outPath)), outPath, blocker)
    }
  }
}

final class SaveParquet[F[_], A: ClassTag](
  rdd: RDD[A],
  outPath: String,
  sma: SaveModeAware[F],
  params: SaverParams)(implicit codec: NJAvroCodec[A], ss: SparkSession)
    extends Serializable {

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    implicit val encoder: AvroEncoder[A] = codec.avroEncoder
    (params.singleOrMulti, params.sparkOrRaw) match {
      case (SingleOrMulti.Single, _) =>
        sma.run(
          rdd.stream[F].through(fileSink[F](blocker).parquet(outPath)).compile.drain,
          outPath,
          blocker)
      case (SingleOrMulti.Multi, SparkOrRaw.Spark) =>
        sma.run(F.delay(savers.parquet(rdd, outPath)), outPath, blocker)
      case (SingleOrMulti.Multi, SparkOrRaw.Raw) =>
        sma.run(F.delay(savers.raw.parquet(rdd, outPath)), outPath, blocker)
    }
  }
}

final class SaveJackson[F[_], A: ClassTag](
  rdd: RDD[A],
  outPath: String,
  sma: SaveModeAware[F],
  params: SaverParams)(implicit codec: NJAvroCodec[A], ss: SparkSession)
    extends Serializable {

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    implicit val encoder: AvroEncoder[A] = codec.avroEncoder
    params.singleOrMulti match {
      case SingleOrMulti.Single =>
        sma.run(
          rdd.stream[F].through(fileSink[F](blocker).jackson(outPath)).compile.drain,
          outPath,
          blocker)
      case SingleOrMulti.Multi =>
        sma.run(F.delay(savers.raw.jackson(rdd, outPath)), outPath, blocker)
    }
  }
}

final class SaveBinaryAvro[F[_], A: ClassTag](
  rdd: RDD[A],
  outPath: String,
  sma: SaveModeAware[F],
  params: SaverParams)(implicit codec: NJAvroCodec[A], ss: SparkSession)
    extends Serializable {

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    implicit val encoder: AvroEncoder[A] = codec.avroEncoder
    sma.run(
      rdd.stream[F].through(fileSink[F](blocker).binAvro(outPath)).compile.drain,
      outPath,
      blocker)
  }
}

final class SaveCirce[F[_], A: ClassTag](
  rdd: RDD[A],
  outPath: String,
  sma: SaveModeAware[F],
  params: SaverParams)(implicit
  jsonEncoder: JsonEncoder[A],
  codec: NJAvroCodec[A],
  ss: SparkSession)
    extends Serializable {

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    params.singleOrMulti match {
      case SingleOrMulti.Single =>
        sma.run(
          rdd.stream[F].through(fileSink[F](blocker).circe(outPath)).compile.drain,
          outPath,
          blocker)
      case SingleOrMulti.Multi =>
        sma.run(F.delay(savers.circe(rdd, outPath)), outPath, blocker)
    }
}

final class SaveCSV[F[_], A: ClassTag](
  rdd: RDD[A],
  outPath: String,
  csvConfiguration: CsvConfiguration,
  sma: SaveModeAware[F],
  params: SaverParams)(implicit rowEncoder: RowEncoder[A], codec: NJAvroCodec[A], ss: SparkSession)
    extends Serializable {

  def updateConfig(f: CsvConfiguration => CsvConfiguration) =
    new SaveCSV[F, A](rdd, outPath, f(csvConfiguration), sma, params)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    params.singleOrMulti match {
      case SingleOrMulti.Single =>
        sma.run(
          rdd.stream[F].through(fileSink[F](blocker).csv(outPath, csvConfiguration)).compile.drain,
          outPath,
          blocker)
      case SingleOrMulti.Multi =>
        val csv = F.delay(
          utils
            .toDF(rdd, codec.avroEncoder)
            .write
            .mode(SaveMode.Overwrite)
            .option("sep", csvConfiguration.cellSeparator.toString)
            .option("header", csvConfiguration.hasHeader)
            .option("quote", csvConfiguration.quote.toString)
            .option("charset", "UTF8")
            .csv(outPath))
        sma.run(csv, outPath, blocker)
    }
}

final class SaveText[F[_], A: ClassTag](
  rdd: RDD[A],
  outPath: String,
  sma: SaveModeAware[F],
  params: SaverParams)(implicit show: Show[A], codec: NJAvroCodec[A], ss: SparkSession)
    extends Serializable {

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    params.singleOrMulti match {
      case SingleOrMulti.Single =>
        sma.run(
          rdd.stream[F].through(fileSink[F](blocker).text(outPath)).compile.drain,
          outPath,
          blocker)
      case SingleOrMulti.Multi =>
        sma.run(F.delay(savers.text(rdd, outPath)), outPath, blocker)
    }
}
