package mtest.spark.kafka

import alleycats.Empty
import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark.kafka.{NJConsumerRecord, NJProducerRecord, SparKafkaTopic}
import frameless.TypedEncoder
import io.circe.generic.auto._
import mtest.spark
import mtest.spark.persist.{Rooster, RoosterData}
import mtest.spark.{sparkSession}
import org.scalatest.funsuite.AnyFunSuite
import cats.effect.unsafe.implicits.global
import java.time.Instant
import scala.concurrent.duration._

class KafkaUploadUnloadTest extends AnyFunSuite {

  implicit val te: TypedEncoder[NJConsumerRecord[Int, Rooster]] = shapeless.cachedImplicit
  val root: String                                              = "./data/test/spark/kafka/load/rooster/"

  val rooster: TopicDef[Int, Rooster] =
    TopicDef[Int, Rooster](TopicName("spark.kafka.load.rooster"), Rooster.avroCodec)
  val topic: SparKafkaTopic[IO, Int, Rooster] = sparKafka.topic(rooster)

  val oac = NJConsumerRecord.avroCodec(rooster)

  val ate = NJConsumerRecord.ate(rooster)

  test("kafka upload/unload") {

    val circe   = root + "circe"
    val parquet = root + "parquet"
    val json    = root + "json"
    val avro    = root + "avro"
    val jackson = root + "jackson"
    val avroBin = root + "avroBin"
    val obj     = root + "objectFile"

    val pr = topic
      .prRdd(RoosterData.data.zipWithIndex.map { case (x, i) =>
        NJProducerRecord[Int, Rooster](x)
          .modifyKey(identity)
          .modifyValue(identity)
          .withKey(i.toInt)
          .withValue(x)
          .withPartition(0)
          .withTimestamp(Instant.now.getEpochSecond * 1000)
          .noPartition
          .noTimestamp
      })
      .noTimestamp
      .noPartition
      .noMeta
      .withBufferSize(1)
      .withRecordsLimit(1000)
      .withTimeLimit(2.minutes)

    val run = for {
      _ <- rooster.in(ctx).admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- pr.uploadByBulk.withBulkSize(1024).run(spark.akkaSystem).compile.drain
      _ <- pr.count.map(println)
      _ <- topic.fromKafka.flatMap(_.save.circe(circe).folder.run)
      _ <- topic.fromKafka.flatMap(_.crDS.save.parquet(parquet).folder.run)
      _ <- topic.fromKafka.flatMap(_.crDS.save.json(json).run)
      _ <- topic.fromKafka.flatMap(_.save.avro(avro).folder.run)
      _ <- topic.fromKafka.flatMap(_.save.jackson(jackson).folder.run)
      _ <- topic.fromKafka.flatMap(_.save.binAvro(avroBin).folder.run)
      _ <- topic.fromKafka.flatMap(_.save.objectFile(obj).run)
    } yield {
      val circeds  = topic.load.circe(circe).dataset.collect().flatMap(_.value).toSet
      val circerdd = topic.load.rdd.circe(circe).rdd.flatMap(_.value).collect().toSet
      val spkJson =
        topic
          .crDS(sparkSession.read.schema(NJConsumerRecord.ate(topic.topic.topicDef).sparkEncoder.schema).json(circe))
          .dataset
          .collect()
          .flatMap(_.value)
          .toSet

      assert(circeds == RoosterData.expected)
      assert(circerdd == RoosterData.expected)
      assert(spkJson == RoosterData.expected)

      val parquetds = topic.load.parquet(parquet).dataset.collect().flatMap(_.value).toSet
      val spkParquet = // can be consumed by spark
        topic.crDS(sparkSession.read.parquet(parquet)).dataset.collect().flatMap(_.value).toSet
      assert(parquetds == RoosterData.expected)
      assert(spkParquet == RoosterData.expected)

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

  test("dump and replay") {
    (topic.dump >> topic.replay >> topic.countDisk >> topic.countKafka).unsafeRunSync()
  }
  test("empty NJProducerRecord") {
    val empty = Empty[NJProducerRecord[Int, Int]]
    assert(empty.empty.partition.isEmpty)
    assert(empty.empty.offset.isEmpty)
    assert(empty.empty.key.isEmpty)
    assert(empty.empty.value.isEmpty)
    assert(empty.empty.timestamp.isEmpty)
  }
}
