package com.github.chenharryhua.nanjin.pipes

import java.io.{EOFException, InputStream, ObjectInputStream, ObjectOutputStream}

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, Resource}
import cats.implicits._
import fs2.io.{readOutputStream, toInputStream}
import fs2.{Pipe, Pull, Stream}

final class JavaObjectSerialization[F[_]: Concurrent: ContextShift, A] {

  def serialize: Pipe[F, A, Byte] = { (ss: Stream[F, A]) =>
    for {
      blocker <- Stream.resource(Blocker[F])
      bs <- readOutputStream[F](blocker, chunkSize) { bos =>
        val oos = new ObjectOutputStream(bos)
        def go(as: Stream[F, A]): Pull[F, Byte, Unit] =
          as.pull.uncons.flatMap {
            case Some((hl, tl)) =>
              Pull.eval(hl.traverse(a => blocker.delay(oos.writeObject(a)))) >> go(tl)
            case None => Pull.eval(blocker.delay(oos.close())) >> Pull.done
          }
        go(ss).stream.compile.drain
      }
    } yield bs
  }
}

final class JavaObjectDeserialization[F[_]: ConcurrentEffect: ContextShift, A] {
  private val F: ConcurrentEffect[F] = ConcurrentEffect[F]

  /**
    * rely on EOFException.. not sure it is the right way
    */
  private def pullAll(ois: ObjectInputStream): Pull[F, A, Option[ObjectInputStream]] =
    Pull
      .functionKInstance(
        F.delay(try Some(ois.readObject().asInstanceOf[A])
        catch { case ex: EOFException => None }))
      .flatMap {
        case Some(a) => Pull.output1(a) >> Pull.pure(Some(ois))
        case None    => Pull.eval(F.delay(ois.close())) >> Pull.pure(None)
      }

  private def readInputStream(is: InputStream): Stream[F, A] =
    for {
      ois <- Stream.resource(Resource.fromAutoCloseable(F.delay(new ObjectInputStream(is))))
      a <- Pull.loop(pullAll)(ois).void.stream
    } yield a

  def deserialize: Pipe[F, Byte, A] = { (ss: Stream[F, Byte]) =>
    ss.through(toInputStream).flatMap(readInputStream)
  }
}
