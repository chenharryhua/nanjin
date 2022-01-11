package com.github.chenharryhua.nanjin.pipes.serde

import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.io.toInputStream
import fs2.{Pipe, Pull, Stream}

import java.io.*

final class JavaObjectSerialization[F[_], A] extends Serializable {

  def serialize(chunkSize: ChunkSize): Pipe[F, A, Byte] = { (ss: Stream[F, A]) =>
    ss.chunkN(chunkSize.value).flatMap { as =>
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
  @SuppressWarnings(Array("AsInstanceOf"))
  private def pullAll(ois: ObjectInputStream)(implicit F: Sync[F]): Pull[F, A, Option[ObjectInputStream]] =
    Pull
      .functionKInstance(
        F.delay(try Some(ois.readObject().asInstanceOf[A])
        catch { case _: EOFException => None }))
      .flatMap {
        case Some(a) => Pull.output1(a) >> Pull.pure(Some(ois))
        case None    => Pull.eval(F.blocking(ois.close())) >> Pull.pure(None)
      }

  private def readInputStream(is: InputStream)(implicit F: Sync[F]): Stream[F, A] =
    for {
      ois <- Stream.resource(
        Resource.make(F.blocking(new ObjectInputStream(is)))(r => F.blocking(r.close()).attempt.void))
      a <- Pull.loop(pullAll)(ois).void.stream
    } yield a

  def deserialize(implicit ce: Async[F]): Pipe[F, Byte, A] = { (ss: Stream[F, Byte]) =>
    ss.through(toInputStream).flatMap(readInputStream)
  }
}
