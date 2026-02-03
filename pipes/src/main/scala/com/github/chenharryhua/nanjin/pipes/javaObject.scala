package com.github.chenharryhua.nanjin.pipes

import cats.effect.kernel.{Async, Sync}
import fs2.io.toInputStream
import fs2.{Pipe, Pull, Stream}

import java.io.*

object javaObject {

  def toBytes[F[_], A]: Pipe[F, A, Byte] = { (ss: Stream[F, A]) =>
    ss.chunks.flatMap { as =>
      val bos = new ByteArrayOutputStream
      val oos = new ObjectOutputStream(bos)
      as.foreach(oos.writeObject)
      oos.close()
      bos.close()
      Stream.emits(bos.toByteArray)
    }
  }

  /** rely on EOFException.. not sure it is the right way
    */
  @SuppressWarnings(Array("AsInstanceOf"))
  private def pullAll[F[_], A](ois: ObjectInputStream)(implicit
    F: Sync[F]): Pull[F, A, Option[ObjectInputStream]] =
    Pull
      .functionKInstance(
        F.delay(try Some(ois.readObject().asInstanceOf[A]) // scalafix:ok

        catch { case _: EOFException => None }))
      .flatMap {
        case Some(a) => Pull.output1[F, A](a) >> Pull.pure[F, Option[ObjectInputStream]](Some(ois))
        case None    => Pull.eval(F.blocking(ois.close())) >> Pull.pure[F, Option[ObjectInputStream]](None)
      }

  private def readInputStream[F[_], A](is: InputStream)(implicit F: Sync[F]): Stream[F, A] =
    for {
      ois <- Stream.bracket(F.blocking(new ObjectInputStream(is)))(r => F.blocking(r.close()))
      a <- Pull.loop(pullAll[F, A])(ois).void.stream
    } yield a

  def fromBytes[F[_], A](implicit ce: Async[F]): Pipe[F, Byte, A] = { (ss: Stream[F, Byte]) =>
    ss.through(toInputStream[F]).flatMap(readInputStream[F, A])
  }

}
