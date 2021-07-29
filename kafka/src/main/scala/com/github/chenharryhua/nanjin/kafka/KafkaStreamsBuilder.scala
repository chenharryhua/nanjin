package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.std.Dispatcher
import cats.effect.kernel.{Async, Deferred}
import cats.syntax.all.*
import cats.{Applicative, Show}
import fs2.Stream
import fs2.concurrent.{Channel, SignallingRef}
import io.circe.{Decoder, Encoder}
import io.scalaland.enumz.Enum
import monocle.function.At.at
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.{KafkaStreams, Topology}

sealed abstract class KafkaStreamException(msg: String) extends Exception(msg)

object KafkaStreamException {
  final case class UncaughtException(ex: Throwable) extends KafkaStreamException(ex.getMessage)
}

final case class KafkaStreamStateChange(newState: State, oldState: State)

object KafkaStreamStateChange {

  implicit val showKafkaStreamsState: Show[State] = Enum[State].getName

  implicit val showKafkaStreamStateUpdate: Show[KafkaStreamStateChange] =
    cats.derived.semiauto.show[KafkaStreamStateChange]

  implicit val encodeState: Encoder[State] = Encoder[String].contramap((s: State) => s.show)
  implicit val decodeState: Decoder[State] = Decoder[String].map(str => Enum[State].withName(str))

  implicit val encodeKafkaStreamStateChange: Encoder[KafkaStreamStateChange] =
    io.circe.generic.semiauto.deriveEncoder[KafkaStreamStateChange]

  implicit val decodeKafkaStreamStateChange: Decoder[KafkaStreamStateChange] =
    io.circe.generic.semiauto.deriveDecoder[KafkaStreamStateChange]
}

final class KafkaStreamsBuilder[F[_]](
  settings: KafkaStreamSettings,
  top: Reader[StreamsBuilder, Unit],
  localStateStores: List[Reader[StreamsBuilder, StreamsBuilder]]) {

  final private class StreamErrorHandler(dispatcher: Dispatcher[F], errorListener: Deferred[F, KafkaStreamException])
      extends StreamsUncaughtExceptionHandler {

    override def handle(throwable: Throwable): StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse = {
      dispatcher.unsafeRunSync(errorListener.complete(KafkaStreamException.UncaughtException(throwable)))
      StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
    }
  }

  final private class StateUpdateEvent(
    dispatcher: Dispatcher[F],
    channel: Channel[F, KafkaStreamStateChange],
    stopSignal: SignallingRef[F, Boolean])(implicit F: Applicative[F])
      extends KafkaStreams.StateListener {

    override def onChange(newState: State, oldState: State): Unit =
      dispatcher.unsafeRunSync(
        channel.send(KafkaStreamStateChange(newState, oldState)) *>
          stopSignal.set(true).whenA(newState == State.NOT_RUNNING))
  }

  def stateStream(implicit F: Async[F]): Stream[F, KafkaStreamStateChange] =
    for {
      dispatcher <- Stream.resource(Dispatcher[F])
      errorListener <- Stream.eval(F.deferred[KafkaStreamException])
      stopSignal <- Stream.eval(SignallingRef.of(false))
      state <- Stream.eval(Channel.unbounded[F, KafkaStreamStateChange]).flatMap { channel =>
        val kss = Stream
          .bracket(F.blocking(new KafkaStreams(topology, settings.javaProperties)))(ks =>
            F.blocking(ks.close()) >> F.blocking(ks.cleanUp()))
          .evalMap(ks =>
            F.blocking {
              ks.cleanUp()
              ks.setUncaughtExceptionHandler(new StreamErrorHandler(dispatcher, errorListener))
              ks.setStateListener(new StateUpdateEvent(dispatcher, channel, stopSignal))
              ks.start()
            }) <* Stream.never[F]
        val error = Stream.eval(errorListener.get.map(_.asLeft[KafkaStreamStateChange])).rethrow
        channel.stream.concurrently(kss).concurrently(error).interruptWhen(stopSignal)
      }
    } yield state

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
