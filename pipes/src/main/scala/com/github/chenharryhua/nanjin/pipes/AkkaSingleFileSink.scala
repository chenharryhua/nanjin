package com.github.chenharryhua.nanjin.pipes

import java.net.URI

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.Sink
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import cats.effect.ConcurrentEffect
import cats.effect.concurrent.Deferred
import cats.implicits._
import com.sksamuel.avro4s.{
  AvroOutputStream,
  AvroOutputStreamBuilder,
  DefaultFieldMapper,
  SchemaFor,
  Encoder => AvroEncoder
}
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

final private class AkkaFileSink[F[_], A](
  pathStr: String,
  schema: Schema,
  hadoopConfig: Configuration,
  builder: AvroOutputStreamBuilder[A])(implicit F: ConcurrentEffect[F])
    extends GraphStageWithMaterializedValue[SinkShape[A], F[NotUsed]] {

  val in: Inlet[A]                 = Inlet[A]("avro.data.in")
  override def shape: SinkShape[A] = SinkShape(in)

  override def createLogicAndMaterializedValue(
    inheritedAttributes: Attributes): (GraphStageLogic, F[NotUsed]) = {
    val deferred = Deferred.unsafe[F, Either[Throwable, NotUsed]]
    val logic: GraphStageLogic with InHandler = new GraphStageLogic(shape) with InHandler {
      private val fs: FileSystem           = FileSystem.get(new URI(pathStr), hadoopConfig)
      private val fos: FSDataOutputStream  = fs.create(new Path(pathStr))
      private val aos: AvroOutputStream[A] = builder.to(fos).build(schema)

      private def closeAll(): Unit = {
        aos.flush()
        aos.close()
        fos.close()
        fs.close()
      }

      override def preStart(): Unit = pull(in)
      override def postStop(): Unit = closeAll()

      override def onPush(): Unit = {
        aos.write(grab(in))
        pull(in)
      }

      override def onUpstreamFailure(t: Throwable): Unit = {
        F.toIO(deferred.complete(Left(t))).unsafeRunSync()
        failStage(t)
      }

      override def onUpstreamFinish(): Unit = {
        F.toIO(deferred.complete(Right(NotUsed))).unsafeRunSync()
        completeStage()
      }

      setHandler(in, this)
    }
    (logic, deferred.get.rethrow)
  }
}

final class AkkaSingleFileSink[F[_]: ConcurrentEffect](configuration: Configuration) {

  def avro[A: AvroEncoder](pathStr: String, schema: Schema): Sink[A, F[NotUsed]] =
    Sink.fromGraph(new AkkaFileSink[F, A](pathStr, schema, configuration, AvroOutputStream.data[A]))

  def avro[A: AvroEncoder: SchemaFor](pathStr: String): Sink[A, F[NotUsed]] =
    avro[A](pathStr, SchemaFor[A].schema(DefaultFieldMapper))

  def avroBinary[A: AvroEncoder](pathStr: String, schema: Schema): Sink[A, F[NotUsed]] =
    Sink.fromGraph(
      new AkkaFileSink[F, A](pathStr, schema, configuration, AvroOutputStream.binary[A]))

  def avroBinary[A: AvroEncoder: SchemaFor](pathStr: String): Sink[A, F[NotUsed]] =
    avroBinary[A](pathStr, SchemaFor[A].schema(DefaultFieldMapper))

  def jackson[A: AvroEncoder](pathStr: String, schema: Schema): Sink[A, F[NotUsed]] =
    Sink.fromGraph(new AkkaFileSink[F, A](pathStr, schema, configuration, AvroOutputStream.json[A]))

  def jackson[A: AvroEncoder: SchemaFor](pathStr: String): Sink[A, F[NotUsed]] =
    jackson[A](pathStr, SchemaFor[A].schema(DefaultFieldMapper))
}
