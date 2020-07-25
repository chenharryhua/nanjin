package com.github.chenharryhua.nanjin.spark

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.mapreduce.AvroJacksonKeyOutputFormat
import com.sksamuel.avro4s.{ToRecord, Encoder => AvroEncoder}
import frameless.cats.implicits._
import io.circe.{Encoder => JsonEncoder}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyOutputFormat}
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

  def dump(pathStr: String)(implicit F: Sync[F]): F[Long] =
    fileSink(blocker).delete(pathStr) >> rddResource.use { data =>
      F.delay(data.saveAsObjectFile(pathStr)).as(data.count())
    }

  private def grPair(data: RDD[A])(implicit
    encoder: AvroEncoder[A]): RDD[(AvroKey[GenericRecord], NullWritable)] = {

    val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job, encoder.schema)
    ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

    data.mapPartitions { rcds =>
      val to = ToRecord[A]
      rcds.map(rcd => (new AvroKey[GenericRecord](to.to(rcd)), NullWritable.get()))
    }
  }

  def avro(pathStr: String)(implicit enc: AvroEncoder[A]): F[Long] =
    fileSink(blocker).delete(pathStr) >> rddResource.use { data =>
      F.delay {
        grPair(data).saveAsNewAPIHadoopFile[AvroKeyOutputFormat[GenericRecord]](pathStr)
        data.count()
      }
    }

  def jackson(pathStr: String)(implicit enc: AvroEncoder[A]): F[Long] =
    fileSink(blocker).delete(pathStr) >> rddResource.use { data =>
      F.delay {
        grPair(data).saveAsNewAPIHadoopFile[AvroJacksonKeyOutputFormat](pathStr)
        data.count()
      }
    }

  def circe(pathStr: String)(implicit enc: JsonEncoder[A]): F[Long] =
    fileSink(blocker).delete(pathStr) >> rddResource.use { data =>
      F.delay {
        rdd.map(enc(_).noSpaces).saveAsTextFile(pathStr)
        data.count()
      }
    }

  def parquet(pathStr: String)(implicit enc: AvroEncoder[A]): F[Long] =
    fileSink(blocker).delete(pathStr) >> rddResource.use { data =>
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
        data.count()
      }
    }
}
