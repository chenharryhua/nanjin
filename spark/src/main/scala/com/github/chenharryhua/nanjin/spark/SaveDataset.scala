package com.github.chenharryhua.nanjin.spark

import cats.effect.{Concurrent, Resource, Sync}
import com.sksamuel.avro4s.{AvroOutputStream, DefaultFieldMapper, Encoder, SchemaFor}
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.avro.Schema
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.{Dataset, SparkSession}

final class SaveDataset[F[_]: Concurrent, A: TypedEncoder](ds: Dataset[A])(
  implicit sparkSession: SparkSession)
    extends Serializable {
  @transient lazy val typedDataset: TypedDataset[A] = TypedDataset.create(ds)

  def saveJackson[B: Encoder: SchemaFor](pathStr: String)(f: A => B): Stream[F, Unit] = {
    val schema: Schema = SchemaFor[B].schema(DefaultFieldMapper)

    val fs: Resource[F, FileSystem] =
      Resource.make(Sync[F].delay(FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)))(
        a => Sync[F].delay(a.close()))

    val outFileStream: Resource[F, FSDataOutputStream] = fs.flatMap(fs =>
      Resource.make(Sync[F].delay(fs.create(new Path(pathStr))))(a => Sync[F].delay(a.close())))

    for {
      os <- Stream.resource(outFileStream).map(AvroOutputStream.json[B].to(_).build(schema))
      data <- typedDataset.stream[F].map(f)
    } yield os.write(data)
  }

  def saveAvro[B: Encoder: SchemaFor](pathStr: String)(f: A => B): Stream[F, Unit] = {
    val schema: Schema = SchemaFor[B].schema(DefaultFieldMapper)

    val fs: Resource[F, FileSystem] =
      Resource.make(Sync[F].delay(FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)))(
        a => Sync[F].delay(a.close()))

    val outFileStream: Resource[F, FSDataOutputStream] = fs.flatMap(fs =>
      Resource.make(Sync[F].delay(fs.create(new Path(pathStr))))(a => Sync[F].delay(a.close())))

    for {
      os <- Stream.resource(outFileStream).map(AvroOutputStream.data[B].to(_).build(schema))
      data <- typedDataset.stream[F].map(f)
    } yield os.write(data)
  }
}
