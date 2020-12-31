package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.kafka.{
  CompulsoryK,
  CompulsoryKV,
  CompulsoryV,
  NJProducerRecord,
  OptionalKV,
  SparKafka
}
import frameless.TypedEncoder
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import io.circe.generic.auto._
import mtest.spark.persist.{Rooster, RoosterData}
import mtest.spark.{blocker, contextShift, ctx, sparkSession, timer}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.util.Random

class KafkaUploadUnloadTest extends AnyFunSuite {
  implicit val te1: TypedEncoder[CompulsoryK[Int, Rooster]]  = shapeless.cachedImplicit
  implicit val te2: TypedEncoder[CompulsoryV[Int, Rooster]]  = shapeless.cachedImplicit
  implicit val te3: TypedEncoder[CompulsoryKV[Int, Rooster]] = shapeless.cachedImplicit

  val sk: SparKafkaContext[IO]                            = sparkSession.alongWith(ctx)
  implicit val te: TypedEncoder[OptionalKV[Int, Rooster]] = shapeless.cachedImplicit
  val root: String                                        = "./data/test/spark/kafka/load/rooster/"

  val rooster: TopicDef[Int, Rooster] =
    TopicDef[Int, Rooster](TopicName("spark.kafka.load.rooster"), Rooster.avroCodec)
  val topic: SparKafka[IO, Int, Rooster] = sk.topic(rooster)

  val oac  = OptionalKV.avroCodec(rooster)
  val kaac = CompulsoryK.avroCodec(rooster)
  val vac  = CompulsoryV.avroCodec(rooster)
  val kvac = CompulsoryKV.avroCodec(rooster)

  val ate   = OptionalKV.ate(rooster)
  val kate  = CompulsoryK.ate(rooster)
  val vate  = CompulsoryV.ate(rooster)
  val kvate = CompulsoryKV.ate(rooster)

  test("kafka upload/unload") {

    val circe   = root + "circe"
    val parquet = root + "parquet"
    val json    = root + "json"
    val avro    = root + "avro"
    val jackson = root + "jackson"
    val avroBin = root + "avroBin"
    val obj     = root + "objectFile"

    val pr = topic.prRdd(RoosterData.rdd.zipWithIndex.map { case (x, i) =>
      NJProducerRecord(Random.nextInt(), x)
        .modifyKey(identity)
        .modifyValue(identity)
        .newKey(i.toInt)
        .newValue(x)
        .newPartition(0)
        .newTimestamp(Instant.now.getEpochSecond * 1000)
        .noPartition
        .noTimestamp
        .noMeta
    })
    val run = for {
      _ <- rooster.in(ctx).admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- pr.noPartition.noTimestamp.batchSize(10).upload.compile.drain
      _ <- pr.count.map(println)
      _ <- topic.fromKafka.save.circe(circe).run(blocker)
      _ <- topic.fromKafka.crDS.save.parquet(parquet).run(blocker)
      _ <- topic.fromKafka.crDS.save.json(json).run(blocker)
      _ <- topic.fromKafka.save.avro(avro).run(blocker)
      _ <- topic.fromKafka.save.jackson(jackson).run(blocker)
      _ <- topic.fromKafka.save.binAvro(avroBin).run(blocker)
      _ <- topic.fromKafka.save.objectFile(obj).run(blocker)
    } yield {
      val circeds  = topic.load.circe(circe).dataset.collect().flatMap(_.value).toSet
      val circerdd = topic.load.rdd.circe(circe).rdd.flatMap(_.value).collect().toSet
      val spkJson =
        topic
          .crDS(sparkSession.read.schema(OptionalKV.ate(topic.topic.topicDef).sparkEncoder.schema).json(circe))
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

  test("compulsoryK") {
    val crs = RoosterData.rdd.zipWithIndex.map { case (x, i) =>
      OptionalKV[Int, Rooster](0, i, Instant.now().getEpochSecond, Some(Random.nextInt()), Some(x), "topic", 0)
    }
    val json    = root + "compulsoryK/json"
    val avro    = root + "compulsoryK/avro"
    val parquet = root + "compulsoryK/parquet"
    val sa = topic
      .crRdd(crs)
      .crDS
      .typedDataset
      .repartition(1)
      .deserialized
      .flatMap(_.toCompulsoryK)
      .save[IO](CompulsoryK.ate(topic.topic.topicDef).avroCodec.avroEncoder)

    sa.json(json).run(blocker).unsafeRunSync()
    assert(topic.load.json(json).dataset.collect().flatMap(_.value).toSet == RoosterData.expected)

    sa.avro(avro).run(blocker).unsafeRunSync()
    assert(topic.load.avro(avro).dataset.collect().flatMap(_.value).toSet == RoosterData.expected)

    sa.parquet(parquet).run(blocker).unsafeRunSync()
    assert(topic.load.parquet(parquet).dataset.collect().flatMap(_.value).toSet == RoosterData.expected)
  }
  test("compulsoryV") {
    val crs = RoosterData.rdd.zipWithIndex.map { case (x, i) =>
      OptionalKV[Int, Rooster](0, i, Instant.now().getEpochSecond, Some(Random.nextInt()), Some(x), "topic", 0)
    }
    val json    = root + "compulsoryV/json"
    val avro    = root + "compulsoryV/avro"
    val parquet = root + "compulsoryV/parquet"
    val sa = topic
      .crRdd(crs)
      .crDS
      .repartition(1)
      .typedDataset
      .repartition(1)
      .deserialized
      .flatMap(_.toCompulsoryV)
      .save[IO](CompulsoryV.ate(topic.topic.topicDef).avroCodec.avroEncoder)

    sa.json(json).run(blocker).unsafeRunSync()
    assert(topic.load.json(json).dataset.collect().flatMap(_.value).toSet == RoosterData.expected)

    sa.avro(avro).run(blocker).unsafeRunSync()
    assert(topic.load.avro(avro).dataset.collect().flatMap(_.value).toSet == RoosterData.expected)

    sa.parquet(parquet).run(blocker).unsafeRunSync()
    assert(topic.load.parquet(parquet).dataset.collect().flatMap(_.value).toSet == RoosterData.expected)
  }

  test("compulsoryKV") {
    val crs = RoosterData.rdd.zipWithIndex.map { case (x, i) =>
      OptionalKV[Int, Rooster](0, i, Instant.now().getEpochSecond, Some(Random.nextInt()), Some(x), "topic", 0)
    }
    val json    = root + "compulsoryKV/json"
    val avro    = root + "compulsoryKV/avro"
    val parquet = root + "compulsoryKV/parquet"
    val sa = topic
      .crRdd(crs)
      .crDS
      .typedDataset
      .repartition(1)
      .deserialized
      .flatMap(_.toCompulsoryKV)
      .save[IO](CompulsoryKV.ate(topic.topic.topicDef).avroCodec.avroEncoder)

    sa.json(json).run(blocker).unsafeRunSync()
    assert(topic.load.json(json).dataset.collect().flatMap(_.value).toSet == RoosterData.expected)

    sa.avro(avro).run(blocker).unsafeRunSync()
    assert(topic.load.avro(avro).dataset.collect().flatMap(_.value).toSet == RoosterData.expected)

    sa.parquet(parquet).run(blocker).unsafeRunSync()
    assert(topic.load.parquet(parquet).dataset.collect().flatMap(_.value).toSet == RoosterData.expected)
  }
}
