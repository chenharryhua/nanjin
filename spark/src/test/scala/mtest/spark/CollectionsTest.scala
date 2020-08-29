package mtest.spark

import java.time.Instant

import cats.effect.IO
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import com.sksamuel.avro4s.{Codec, Decoder, Encoder, SchemaFor}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite
import cats.implicits._
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.sql.SaveMode

object CollectionsTestData {

  object AntLeg extends Enumeration {
    val A, B, C, D, E, F = Value
  }

  final case class AntList(index: Int, al: List[AntLeg.Value], now: Instant)
  final case class AntArray(index: Int, al: Array[AntLeg.Value], now: Instant)
  final case class AntVector(index: Int, al: Vector[AntLeg.Value], now: Instant)

  final case class SimpleList(index: Int, a: List[Int], now: Instant)

  val antList: List[AntList] = List(
    AntList(1, List(AntLeg.C, AntLeg.D), Instant.now()),
    AntList(2, List(AntLeg.C, AntLeg.D), Instant.now()),
    AntList(3, List(), Instant.now())
  )

  val antArray: List[AntArray] = List(
    AntArray(1, Array(AntLeg.C, AntLeg.D), Instant.now()),
    AntArray(2, Array(AntLeg.C, AntLeg.D), Instant.now()),
    AntArray(3, Array(), Instant.now())
  )

  val antVector: List[AntVector] = List(
    AntVector(1, Vector(AntLeg.C, AntLeg.D), Instant.now()),
    AntVector(2, Vector(AntLeg.C, AntLeg.D), Instant.now()),
    AntVector(3, Vector(), Instant.now())
  )

  val simple: List[SimpleList] = List(
    SimpleList(1, List(1), Instant.now()),
    SimpleList(2, List(1, 2), Instant.now()),
    SimpleList(3, List(1, 2, 3), Instant.now()),
    SimpleList(4, List(1, 2), Instant.now()),
    SimpleList(5, List(1), Instant.now()),
    SimpleList(6, List(), Instant.now())
  )

  val antListData: RDD[AntList]   = sparkSession.sparkContext.parallelize(antList)
  val simpleData: RDD[SimpleList] = sparkSession.sparkContext.parallelize(simple)

  implicit val ate: TypedEncoder[AntList]    = shapeless.cachedImplicit
  implicit val avroEncoder: Encoder[AntList] = shapeless.cachedImplicit
  implicit val avroDecoder: Decoder[AntList] = shapeless.cachedImplicit

  implicit val sate: TypedEncoder[SimpleList]    = shapeless.cachedImplicit
  implicit val savroEncoder: Encoder[SimpleList] = shapeless.cachedImplicit
  implicit val savroDecoder: Decoder[SimpleList] = shapeless.cachedImplicit

  implicit val avroTypedEncoder: AvroTypedEncoder[SimpleList] =
    new AvroTypedEncoder[SimpleList](sate, NJAvroCodec[SimpleList])
}

class CollectionsTest extends AnyFunSuite {
  import CollectionsTestData._

  test("hadoop avro multi") {
    val path = "./data/test/spark/collection/multi.avro"
    val run = for {
      _ <- simpleData.save[IO].hadoopAvro(path).multi.repartition(1).run(blocker)
      r <- IO(sparkSession.load.avro[SimpleList](path).take(100))
    } yield assert(r.toSet == simple.toSet)
    run.unsafeRunSync()
  }

  test("spark avro multi") {
    import sparkSession.implicits._
    val path = "./data/test/spark/collection/spark.avro"
    val run = for {
      _ <- IO(
        avroTypedEncoder
          .fromRDD(simpleData, sparkSession)
          .write
          .mode(SaveMode.Overwrite)
          .format("avro")
          .save(path))
      _ <- simpleData.save[IO].avro(path).multi.repartition(1).run(blocker)
      r <-
        TypedDataset
          .createUnsafe[SimpleList](sparkSession.read.format("avro").load(path))
          .collect[IO]()
    } yield assert(r.toSet == simple.toSet)
    run.unsafeRunSync()
  }

  test("avro single") {
    val path = "./data/test/spark/collection/single.avro"
    val run = for {
      _ <- simpleData.save[IO].avro(path).single.run(blocker)
      r <- IO(sparkSession.load.avro[SimpleList](path).take(100))
    } yield assert(r.toSet == simple.toSet)
    run.unsafeRunSync()
  }
  /*
  test("avro partition") {
    val path = "./data/test/spark/collection/partition.avro"
    val run = for {
      _ <-
        simpleData
          .save[IO]
          .avro(x => Some(x.index))(i => s"$path/$i")
          .repartition(1)
          .run(blocker)
      r1 <- IO(sparkSession.load.avro[SimpleList](path + "/1").take(100))
      r2 <- IO(sparkSession.load.avro[SimpleList](path + "/2").take(100))
      r3 <- IO(sparkSession.load.avro[SimpleList](path + "/3").take(100))
      r4 <- IO(sparkSession.load.avro[SimpleList](path + "/4").take(100))
      r5 <- IO(sparkSession.load.avro[SimpleList](path + "/5").take(100))
      r6 <- IO(sparkSession.load.avro[SimpleList](path + "/6").take(100))
    } yield assert((r1 ++ r2 ++ r3 ++ r4 ++ r5 ++ r6).toSet == simple.toSet)
    run.unsafeRunSync()
  }

   */

}
