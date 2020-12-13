package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.kafka.{NJProducerRecord, OptionalKV}
import frameless.TypedEncoder
import io.circe.generic.auto._
import mtest.spark.persist.{Rooster, RoosterData}
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class KafkaUploadUnloadTest extends AnyFunSuite {

  val sk: SparkWithKafkaContext[IO]                       = sparkSession.alongWith(ctx)
  implicit val te: TypedEncoder[OptionalKV[Int, Rooster]] = shapeless.cachedImplicit
  val root                                                = "./data/test/spark/kafka/load/rooster/"

  test("kafka upload/unload") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("spark.kafka.load.rooster"), Rooster.avroCodec)
    val circe   = root + "circe"
    val parquet = root + "parquet"
    val json    = root + "json"
    val avro    = root + "avro"
    val jackson = root + "jackson"
    val avroBin = root + "avroBin"
    val obj     = root + "objectFile"

    val topic = sk.topic(rooster)
    val pr    = topic.prRdd(RoosterData.rdd.map(x => NJProducerRecord(Random.nextInt(), x)))
    val run = for {
      _ <- rooster.in(ctx).admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- pr.upload.compile.drain
      _ <- topic.fromKafka.flatMap(_.save.circe(circe).run(blocker))
      _ <- topic.fromKafka.flatMap(_.crDS.save.parquet(parquet).run(blocker))
      _ <- topic.fromKafka.flatMap(_.crDS.save.json(json).run(blocker))
      _ <- topic.fromKafka.flatMap(_.save.avro(avro).run(blocker))
      _ <- topic.fromKafka.flatMap(_.save.jackson(jackson).run(blocker))
      _ <- topic.fromKafka.flatMap(_.save.binAvro(avroBin).run(blocker))
      _ <- topic.fromKafka.flatMap(_.save.objectFile(obj).run(blocker))
    } yield {
      val circeds  = topic.load.circe(circe).dataset.collect().flatMap(_.value).toSet
      val circerdd = topic.load.rdd.circe(circe).rdd.flatMap(_.value).collect().toSet
      assert(circeds == RoosterData.expected)
      assert(circerdd == RoosterData.expected)

      val parquetds = topic.load.parquet(parquet).dataset.collect().flatMap(_.value).toSet
      assert(parquetds == RoosterData.expected)

      val jsonds = topic.load.json(json).dataset.collect().flatMap(_.value).toSet
      assert(jsonds == RoosterData.expected)

      val avrods  = topic.load.avro(avro).dataset.collect().flatMap(_.value).toSet
      val avrordd = topic.load.rdd.avro(avro).rdd.flatMap(_.value).collect().toSet
      assert(avrods == RoosterData.expected)
      assert(avrordd == RoosterData.expected)

      val jacksonds  = topic.load.jackson(jackson).dataset.collect().flatMap(_.value).toSet
      val jacksonrdd = topic.load.rdd.jackson(jackson).rdd.flatMap(_.value).collect().toSet
      assert(jacksonds == RoosterData.expected)
      assert(jacksonrdd == RoosterData.expected)

      val binds  = topic.load.binAvro(avroBin).dataset.collect().flatMap(_.value).toSet
      val binrdd = topic.load.rdd.binAvro(avroBin).rdd.flatMap(_.value).collect().toSet
      assert(binds == RoosterData.expected)
      assert(binrdd == RoosterData.expected)

      val objds  = topic.load.objectFile(obj).dataset.collect().flatMap(_.value).toSet
      val objrdd = topic.load.rdd.objectFile(obj).rdd.flatMap(_.value).collect().toSet
      assert(objds == RoosterData.expected)
      assert(objrdd == RoosterData.expected)
    }
    run.unsafeRunSync()
  }
}
