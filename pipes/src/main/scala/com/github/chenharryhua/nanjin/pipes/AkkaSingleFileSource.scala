package com.github.chenharryhua.nanjin.pipes

import java.net.URI

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import cats.implicits._
import com.sksamuel.avro4s.{
  AvroInputStream,
  AvroInputStreamBuilder,
  Decoder,
  DefaultFieldMapper,
  SchemaFor
}
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

final private class AkkaFileSource[A](
  pathStr: String,
  schema: Schema,
  hadoopConfig: Configuration,
  builder: AvroInputStreamBuilder[A])
    extends GraphStage[SourceShape[A]] {

  private val out: Outlet[A] = Outlet[A]("avro.data.out")

  override val shape: SourceShape[A] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

    val fs: FileSystem          = FileSystem.get(new URI(pathStr), hadoopConfig)
    val fis: FSDataInputStream  = fs.open(new Path(pathStr))
    val ais: AvroInputStream[A] = builder.from(fis).build(schema)

    val iterator = ais.iterator

    def closeAll(): Unit = {
      ais.close()
      fis.close()
      fs.close()
    }
    new GraphStageLogic(shape) with OutHandler {

      setHandler(out, this)

      override def onPull(): Unit =
        if (iterator.hasNext) emit(out, iterator.next()) else completeStage()

      override def postStop(): Unit = closeAll()
    }
  }
}

final class AkkaSingleFileSource(configuration: Configuration) {

  def avro[A: Decoder](pathStr: String, schema: Schema): Source[A, NotUsed] =
    Source.fromGraph(new AkkaFileSource[A](pathStr, schema, configuration, AvroInputStream.data[A]))

  def avro[A: Decoder: SchemaFor](pathStr: String): Source[A, NotUsed] =
    avro[A](pathStr, SchemaFor[A].schema(DefaultFieldMapper))

  def avroBinary[A: Decoder](pathStr: String, schema: Schema): Source[A, NotUsed] =
    Source.fromGraph(
      new AkkaFileSource[A](pathStr, schema, configuration, AvroInputStream.binary[A]))

  def avroBinary[A: Decoder: SchemaFor](pathStr: String): Source[A, NotUsed] =
    avroBinary[A](pathStr, SchemaFor[A].schema(DefaultFieldMapper))

  def jackson[A: Decoder](pathStr: String, schema: Schema): Source[A, NotUsed] =
    Source.fromGraph(new AkkaFileSource[A](pathStr, schema, configuration, AvroInputStream.json[A]))

  def jackson[A: Decoder: SchemaFor](pathStr: String): Source[A, NotUsed] =
    jackson[A](pathStr, SchemaFor[A].schema(DefaultFieldMapper))
}
