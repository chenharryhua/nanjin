package com.github.chenharryhua.nanjin.pipes

import java.net.URI

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.sksamuel.avro4s.{
  AvroOutputStream,
  AvroOutputStreamBuilder,
  DefaultFieldMapper,
  SchemaFor,
  Encoder => AvroEncoder
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

final class AkkaFileSink[A: SchemaFor: AvroEncoder](
  pathStr: String,
  hadoopConfig: Configuration,
  builder: AvroOutputStreamBuilder[A])
    extends GraphStage[SinkShape[A]] {

  private val fs: FileSystem          = FileSystem.get(new URI(pathStr), hadoopConfig)
  private val fos: FSDataOutputStream = fs.create(new Path(pathStr))

  private val aos: AvroOutputStream[A] =
    builder.to(fos).build(SchemaFor[A].schema(DefaultFieldMapper))

  private def closeAll(): Unit = {
    aos.flush()
    aos.close()
    fos.close()
    fs.close()
  }

  private val in: Inlet[A] = Inlet[A]("avro.data.in")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      override def preStart(): Unit = pull(in)

      setHandler(
        in,
        new InHandler {

          override def onPush(): Unit = {
            aos.write(grab(in))
            pull(in)
          }

          override def onUpstreamFinish(): Unit = {
            closeAll()
            completeStage()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            closeAll()
            failStage(ex)
          }
        }
      )
    }

  override def shape: SinkShape[A] = SinkShape(in)
}

final class AkkaSingleFileSink(configuration: Configuration) {

  def avro[A: SchemaFor: AvroEncoder](pathStr: String): AkkaFileSink[A] =
    new AkkaFileSink[A](pathStr, configuration, AvroOutputStream.data[A])

  def jackson[A: SchemaFor: AvroEncoder](pathStr: String): AkkaFileSink[A] =
    new AkkaFileSink[A](pathStr, configuration, AvroOutputStream.json[A])

}
