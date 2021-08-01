package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.kernel.{Async, Deferred, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import fs2.Stream
import monocle.function.At.at
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.{KafkaStreams, Topology}

final case class KafkaStreamsException(msg: String) extends Exception(msg)

final class KafkaStreamsBuilder[F[_]] private (
  settings: KafkaStreamSettings,
  top: Reader[StreamsBuilder, Unit],
  localStateStores: List[Reader[StreamsBuilder, StreamsBuilder]])(implicit F: Async[F]) {
  final private class StreamErrorHandler(dispatcher: Dispatcher[F], errorListener: Deferred[F, KafkaStreamsException])
      extends StreamsUncaughtExceptionHandler {

    override def handle(throwable: Throwable): StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse = {
      dispatcher.unsafeRunSync(errorListener.complete(KafkaStreamsException(ExceptionUtils.getMessage(throwable))))
      StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
    }
  }

  final private class StateUpdateEvent(dispatcher: Dispatcher[F], stopSignal: Deferred[F, KafkaStreamsException])
      extends KafkaStreams.StateListener {

    override def onChange(newState: State, oldState: State): Unit =
      dispatcher.unsafeRunSync(
        stopSignal.complete(KafkaStreamsException("Kafka streams were stopped")).whenA(newState == State.NOT_RUNNING))
  }

  private def kickoff(
    dispatcher: Dispatcher[F],
    errorListener: Deferred[F, KafkaStreamsException],
    stopSignal: Deferred[F, KafkaStreamsException]): Resource[F, KafkaStreams] =
    Resource
      .make(F.blocking(new KafkaStreams(topology, settings.javaProperties)))(ks =>
        F.blocking(ks.close()) >> F.blocking(ks.cleanUp()))
      .evalMap(ks =>
        F.blocking(ks.cleanUp()) >>
          F.blocking {
            ks.setUncaughtExceptionHandler(new StreamErrorHandler(dispatcher, errorListener))
            ks.setStateListener(new StateUpdateEvent(dispatcher, stopSignal))
            ks.start()
            ks
          })

  def stream: Stream[F, Nothing] = {
    val exec: Resource[F, KafkaStreamsException] = for {
      dispatcher <- Dispatcher[F]
      errorListener <- Resource.eval(F.deferred[KafkaStreamsException])
      stopSignal <- Resource.eval(F.deferred[KafkaStreamsException])
      _ <- kickoff(dispatcher, errorListener, stopSignal)
      ex <- Resource.eval(F.race(errorListener.get, stopSignal.get)).map(_.fold(identity, identity))
    } yield ex

    Stream.resource(exec.evalMap(F.raiseError))
  }

  /** cancel friendly
    */
  def query: Stream[F, KafkaStreams] = {
    val ssks: Stream[F, Stream[F, KafkaStreams]] = for {
      dispatcher <- Stream.resource(Dispatcher[F])
      errorListener <- Stream.eval(F.deferred[KafkaStreamsException])
      stopSignal <- Stream.eval(F.deferred[KafkaStreamsException])
      ks <- Stream.resource(kickoff(dispatcher, errorListener, stopSignal))
    } yield {
      val err = F.race(errorListener.get, stopSignal.get).map(_.fold(identity, identity))
      Stream(ks).concurrently(Stream.eval(err.flatMap[Nothing](F.raiseError)))
    }

    ssks.flatten
  }

  def withProperty(key: String, value: String): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      KafkaStreamSettings.config.composeLens(at(key)).set(Some(value))(settings),
      top,
      localStateStores)

  def addStateStore[S <: StateStore](storeBuilder: StoreBuilder[S]): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      settings,
      top,
      Reader((sb: StreamsBuilder) => new StreamsBuilder(sb.addStateStore(storeBuilder))) :: localStateStores)

  def topology: Topology = {
    val builder: StreamsBuilder = new StreamsBuilder()
    val lss: StreamsBuilder     = localStateStores.foldLeft(builder)((bd, rd) => rd.run(bd))
    top.run(lss)
    builder.build()
  }
}

object KafkaStreamsBuilder {
  def apply[F[_]: Async](settings: KafkaStreamSettings, top: Reader[StreamsBuilder, Unit]): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](settings, top, Nil)
}
