package mtest.spark.persist

import cats.Applicative
import cats.syntax.all._
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import io.circe.Codec
import io.circe.shapes._
import monocle.Traversal
import monocle.function.Plated
import monocle.generic.all.coProductPrism
import org.apache.spark.rdd.RDD
import shapeless.{:+:, CNil, Coproduct}
import mtest.spark._
 
final case class Fractual(value: Option[Fractual.FType])

object Fractual {
  type FType = Int :+: String :+: List[Fractual] :+: Map[String, Fractual] :+: CNil

  val avroCodec: AvroCodec[Fractual] = AvroCodec[Fractual]

  implicit val json: Codec[Fractual] = io.circe.generic.semiauto.deriveCodec[Fractual]

  implicit val platedFractual: Plated[Fractual] =
    Plated[Fractual](new Traversal[Fractual, Fractual] {

      override def modifyF[F[_]](f: Fractual => F[Fractual])(s: Fractual)(implicit
        ev: Applicative[F]): F[Fractual] =
        s.value match {
          case None => ev.pure(Fractual(None))
          case Some(pl) =>
            val list = coProductPrism[FType, List[Fractual]]
              .getOption(pl)
              .map(_.traverse(f).map(x => Fractual(Some(Coproduct[FType](x)))))

            val map = coProductPrism[FType, Map[String, Fractual]].getOption(pl).map {
              _.map { case (s, j) => ev.map(f(j))(a => s -> a) }.toList.sequence.map { p =>
                Fractual(Some(Coproduct[FType](p.toMap)))
              }
            }
            list.orElse(map).getOrElse(ev.pure(Fractual(Some(pl))))
        }
    })
}

object FractualData {
  val int     = Fractual(Some(Coproduct[Fractual.FType](1)))
  val string  = Fractual(Some(Coproduct[Fractual.FType]("hello world")))
  val list    = Fractual(Some(Coproduct[Fractual.FType](List(int, string))))
  val map     = Fractual(Some(Coproduct[Fractual.FType](Map("a" -> int, "b" -> string, "c" -> list))))
  val complex = Fractual(Some(Coproduct[Fractual.FType](Map("int" -> int, "map" -> map))))

  val data: List[Fractual] = List(int, string, list, map, complex)

  val rdd: RDD[Fractual] = sparkSession.sparkContext.parallelize(data)

}
