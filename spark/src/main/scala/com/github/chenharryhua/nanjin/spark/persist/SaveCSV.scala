package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.{fileSink, utils, RddExt}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class SaveCSV[F[_], A: ClassTag](
  rdd: RDD[A],
  outPath: String,
  csvConfiguration: CsvConfiguration,
  sma: SaveModeAware[F],
  cfg: SaverConfig)(implicit rowEncoder: RowEncoder[A], codec: NJAvroCodec[A], ss: SparkSession)
    extends Serializable {
  val params: SaverParams = cfg.evalConfig

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
