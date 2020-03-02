package com.github.chenharryhua.nanjin.pipes

import java.net.URI

import akka.stream._
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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import akka.stream.scaladsl.Sink

final private class AkkaFileSink[F[_], A: SchemaFor: AvroEncoder](
  pathStr: String,
  hadoopConfig: Configuration,
  builder: AvroOutputStreamBuilder[A])(implicit F: ConcurrentEffect[F])
    extends GraphStageWithMaterializedValue[SinkShape[A], F[Unit]] {

  val in: Inlet[A]                 = Inlet[A]("avro.data.in")
  override def shape: SinkShape[A] = SinkShape(in)

  override def createLogicAndMaterializedValue(
    inheritedAttributes: Attributes): (GraphStageLogic, F[Unit]) = {
    val deferred = Deferred.unsafe[F, Either[Exception, Unit]]
    val logic: GraphStageLogic with InHandler = new GraphStageLogic(shape) with InHandler {
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

      override def preStart(): Unit = pull(in)
      override def postStop(): Unit = closeAll()

      override def onPush(): Unit = {
        aos.write(grab(in))
        pull(in)
      }

      override def onUpstreamFailure(t: Throwable): Unit = {
        F.toIO(deferred.complete(Left(new Exception("upstream failure")))).unsafeRunSync()
        failStage(t)
      }

      override def onUpstreamFinish(): Unit = {
        F.toIO(deferred.complete(Right(()))).unsafeRunSync()
        completeStage()
      }

      setHandler(in, this)
    }
    (logic, deferred.get.rethrow)
  }
}

final class AkkaSingleFileSink[F[_]: ConcurrentEffect](configuration: Configuration) {

  def avro[A: SchemaFor: AvroEncoder](pathStr: String): Sink[A, F[Unit]] =
    Sink.fromGraph(new AkkaFileSink[F, A](pathStr, configuration, AvroOutputStream.data[A]))

  def avroBinary[A: SchemaFor: AvroEncoder](pathStr: String): Sink[A, F[Unit]] =
    Sink.fromGraph(new AkkaFileSink[F, A](pathStr, configuration, AvroOutputStream.binary[A]))

  def jackson[A: SchemaFor: AvroEncoder](pathStr: String): Sink[A, F[Unit]] =
    Sink.fromGraph(new AkkaFileSink[F, A](pathStr, configuration, AvroOutputStream.json[A]))

}
