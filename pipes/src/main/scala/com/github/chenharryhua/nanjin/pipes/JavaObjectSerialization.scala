package com.github.chenharryhua.nanjin.pipes

import java.io._

import cats.effect.{ConcurrentEffect, Resource, Sync}
import fs2.io.toInputStream
import fs2.{Pipe, Pull, Stream}

final class JavaObjectSerialization[F[_], A] extends Serializable {

  def serialize: Pipe[F, A, Byte] = { (ss: Stream[F, A]) =>
    ss.chunkN(chunkSize).flatMap { as =>
      val bos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(bos)
      as.foreach(oos.writeObject)
      oos.close()
      bos.close()
      Stream.emits(bos.toByteArray)
    }
  }

  /** rely on EOFException.. not sure it is the right way
    */
  private def pullAll(ois: ObjectInputStream)(implicit F: Sync[F]): Pull[F, A, Option[ObjectInputStream]] =
    Pull
      .functionKInstance(
        F.delay(try Some(ois.readObject().asInstanceOf[A])
        catch { case ex: EOFException => None }))
      .flatMap {
        case Some(a) => Pull.output1(a) >> Pull.pure(Some(ois))
        case None    => Pull.eval(F.delay(ois.close())) >> Pull.pure(None)
      }

  private def readInputStream(is: InputStream)(implicit F: Sync[F]): Stream[F, A] =
    for {
      ois <- Stream.resource(Resource.fromAutoCloseable(F.delay(new ObjectInputStream(is))))
      a <- Pull.loop(pullAll)(ois).void.stream
    } yield a

  def deserialize(implicit ce: ConcurrentEffect[F]): Pipe[F, Byte, A] = { (ss: Stream[F, Byte]) =>
    ss.through(toInputStream).flatMap(readInputStream)
  }
}
