package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.concurrent.Deferred
import cats.effect.{ConcurrentEffect, ExitCase}
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.monadError._
import fs2.Stream
import monocle.function.At.at
import org.apache.kafka.streams.processor.{ProcessorSupplier, StateStore}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.{KafkaStreams, Topology}

final case class UncaughtKafkaStreamingException(thread: Thread, ex: Throwable) extends Exception(ex.getMessage)

final case class KafkaStreamingStartupException() extends Exception("failed to start kafka streaming")

final class KafkaStreamsBuilder[F[_]](
  settings: KafkaStreamSettings,
  top: Reader[StreamsBuilder, Unit],
  localStateStores: List[Reader[StreamsBuilder, StreamsBuilder]],
  globalStateStores: List[Reader[StreamsBuilder, StreamsBuilder]])
    extends Serializable {

  final private class StreamErrorHandler(deferred: Deferred[F, UncaughtKafkaStreamingException], F: ConcurrentEffect[F])
      extends Thread.UncaughtExceptionHandler {

    override def uncaughtException(t: Thread, e: Throwable): Unit =
      F.toIO(deferred.complete(UncaughtKafkaStreamingException(t, e))).void.unsafeRunSync()
  }

  final private class Latch(value: Deferred[F, Either[KafkaStreamingStartupException, Unit]], F: ConcurrentEffect[F])
      extends KafkaStreams.StateListener {

    override def onChange(newState: KafkaStreams.State, oldState: KafkaStreams.State): Unit =
      newState match {
        case KafkaStreams.State.RUNNING =>
          F.toIO(value.complete(Right(()))).attempt.void.unsafeRunSync()
        case KafkaStreams.State.ERROR =>
          F.toIO(value.complete(Left(KafkaStreamingStartupException()))).attempt.void.unsafeRunSync()
        case _ => ()
      }
  }

  def run(implicit F: ConcurrentEffect[F]): Stream[F, KafkaStreams] =
    for {
      errorListener <- Stream.eval(Deferred[F, UncaughtKafkaStreamingException])
      latch <- Stream.eval(Deferred[F, Either[KafkaStreamingStartupException, Unit]])
      kss <-
        Stream
          .bracketCase(F.delay(new KafkaStreams(topology, settings.javaProperties))) { case (ks, reason) =>
            reason match {
              case ExitCase.Canceled  => F.delay(ks.close())
              case ExitCase.Completed => F.delay(ks.close())
              case ExitCase.Error(ex) => F.delay(ks.close()) >> F.delay(ks.cleanUp())
            }
          }
          .evalMap(ks =>
            F.delay {
              ks.setUncaughtExceptionHandler(new StreamErrorHandler(errorListener, F))
              ks.setStateListener(new Latch(latch, F))
              ks.start()
            }.as(ks))
          .concurrently(Stream.eval(errorListener.get).flatMap(Stream.raiseError[F]))
      _ <- Stream.eval(latch.get.rethrow)
    } yield kss

  def withProperty(key: String, value: String): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      KafkaStreamSettings.config.composeLens(at(key)).set(Some(value))(settings),
      top,
      localStateStores,
      globalStateStores)

  def addStateStore(storeBuilder: StoreBuilder[_ <: StateStore]): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      settings,
      top,
      Reader((sb: StreamsBuilder) => new StreamsBuilder(sb.addStateStore(storeBuilder))) :: localStateStores,
      globalStateStores)

  def addGlobalStore[K, V](
    storeBuilder: StoreBuilder[_ <: StateStore],
    topic: String,
    consumed: Consumed[K, V],
    stateUpdateSupplier: ProcessorSupplier[K, V]): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      settings,
      top,
      localStateStores,
      Reader((sb: StreamsBuilder) =>
        new StreamsBuilder(sb.addGlobalStore(storeBuilder, topic, consumed, stateUpdateSupplier)))
        :: globalStateStores)

  def topology: Topology = {
    val builder: StreamsBuilder = new StreamsBuilder()
    val lss: StreamsBuilder     = localStateStores.foldLeft(builder)((s, i) => i(s))
    val gss: StreamsBuilder     = globalStateStores.foldLeft(lss)((s, i) => i(s))
    top.run(gss)
    builder.build()
  }
}
