package com.github.chenharryhua.nanjin.spark

import cats.Show
import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.mapreduce.{
  NJAvroKeyOutputFormat,
  NJJacksonKeyOutputFormat
}
import com.sksamuel.avro4s.{ToRecord, Encoder => AvroEncoder}
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.CsvConfiguration
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetOutputFormat, GenericDataSupplier}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

final class RddPersistMultiFile[F[_], A](rdd: RDD[A], blocker: Blocker)(implicit
  ss: SparkSession,
  cs: ContextShift[F],
  F: Sync[F]) {

  private def rddResource(implicit F: Sync[F]): Resource[F, RDD[A]] =
    Resource.make(F.delay(rdd.persist()))(r => F.delay(r.unpersist()))

// 0
  def dump(pathStr: String)(implicit F: Sync[F]): F[Long] =
    rddResource.use { data =>
      fileSink(blocker).delete(pathStr) >>
        F.delay(data.saveAsObjectFile(pathStr)).as(data.count())
    }

  private def grPair(
    data: RDD[A],
    enc: AvroEncoder[A]): RDD[(AvroKey[GenericRecord], NullWritable)] = {

    val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job, enc.schema)
    ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

    data.mapPartitions { rcds =>
      val to = ToRecord[A](enc)
      rcds.map(rcd => (new AvroKey[GenericRecord](to.to(rcd)), NullWritable.get()))
    }
  }

// 1
  def avro(pathStr: String)(implicit enc: AvroEncoder[A]): F[Long] =
    rddResource.use { data =>
      fileSink(blocker).delete(pathStr) >>
        F.delay(grPair(data, enc).saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](pathStr))
          .as(data.count())
    }

// 2
  def jackson(pathStr: String)(implicit enc: AvroEncoder[A]): F[Long] =
    rddResource.use { data =>
      fileSink(blocker).delete(pathStr) >>
        F.delay(grPair(data, enc).saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](pathStr))
          .as(data.count())
    }

// 3
  def parquet(pathStr: String)(implicit enc: AvroEncoder[A], constraint: TypedEncoder[A]): F[Long] =
    rddResource.use { data =>
      fileSink(blocker).delete(pathStr) >>
        F.delay {
          val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
          AvroParquetOutputFormat.setAvroDataSupplier(job, classOf[GenericDataSupplier])
          AvroParquetOutputFormat.setSchema(job, enc.schema)
          ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
          data // null as java Void
            .map(a => (null, enc.encode(a).asInstanceOf[GenericRecord]))
            .saveAsNewAPIHadoopFile(
              pathStr,
              classOf[Void],
              classOf[GenericRecord],
              classOf[AvroParquetOutputFormat[GenericRecord]])
        }.as(data.count())
    }

// 4
  def circe(pathStr: String)(implicit enc: JsonEncoder[A]): F[Long] =
    rddResource.use { data =>
      fileSink(blocker).delete(pathStr) >>
        F.delay(rdd.map(enc(_).noSpaces).saveAsTextFile(pathStr)).as(data.count())
    }

// 5
  def text(pathStr: String)(implicit enc: Show[A]): F[Long] =
    rddResource.use { data =>
      fileSink(blocker).delete(pathStr) >>
        F.delay(rdd.map(enc.show).saveAsTextFile(pathStr)).as(data.count())
    }

// 6
  def csv(pathStr: String, csvConfig: CsvConfiguration)(implicit enc: TypedEncoder[A]): F[Long] =
    rddResource.use { data =>
      val tds = TypedDataset.create(data)
      fileSink(blocker).delete(pathStr) >>
        F.delay(
          tds.write
            .option("sep", csvConfig.cellSeparator.toString)
            .option("header", csvConfig.hasHeader)
            .option("quote", csvConfig.quote.toString)
            .option("charset", "UTF8")
            .csv(pathStr)) >>
        tds.count[F]()
    }

  def csv(pathStr: String)(implicit enc: TypedEncoder[A]): F[Long] =
    csv(pathStr, CsvConfiguration.rfc)

}
