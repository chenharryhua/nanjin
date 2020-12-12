package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.kafka.{NJProducerRecord, OptionalKV}
import frameless.TypedEncoder
import io.circe.generic.auto._
import mtest.spark.persist.{Rooster, RoosterData}
import org.scalatest.funsuite.AnyFunSuite

class KafkaUploadUnloadTest extends AnyFunSuite {

  val sk: SparkWithKafkaContext[IO]                       = sparkSession.alongWith(ctx)
  implicit val te: TypedEncoder[OptionalKV[Int, Rooster]] = shapeless.cachedImplicit

  test("circe upload/unload") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("spark.kafka.load.circe.rooster"), Rooster.avroCodec)
    val path  = "./data/test/spark/kafka/load/circe/rooster"
    val topic = sk.topic(rooster)
    val pr    = topic.prRdd(RoosterData.rdd.map(x => NJProducerRecord(1, x)))
    val run = for {
      _ <- rooster.in(ctx).admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- pr.upload.compile.drain
      _ <- topic.fromKafka.flatMap(_.save.circe(path).run(blocker))
    } yield {
      val ds  = topic.load.circe(path).dataset.collect().flatMap(_.value).toSet
      val rdd = topic.load.rdd.circe(path).rdd.flatMap(_.value).collect().toSet
      assert(ds == RoosterData.expected)
      assert(rdd == RoosterData.expected)
    }
    run.unsafeRunSync()
  }

  test("parquet upload/unload") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("spark.kafka.load.parquet.rooster"), Rooster.avroCodec)
    val path  = "./data/test/spark/kafka/load/parquet/rooster"
    val topic = sk.topic(rooster)
    val pr    = topic.prRdd(RoosterData.rdd.map(x => NJProducerRecord(1, x)))
    val run = for {
      _ <- rooster.in(ctx).admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- pr.upload.compile.drain
      _ <- topic.fromKafka.flatMap(_.crDS.save.parquet(path).run(blocker))
    } yield {
      val ds = topic.load.parquet(path).dataset.collect().flatMap(_.value).toSet
      assert(ds == RoosterData.expected)
    }
    run.unsafeRunSync()
  }

  test("json upload/unload") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("spark.kafka.load.json.rooster"), Rooster.avroCodec)
    val path  = "./data/test/spark/kafka/load/json/rooster"
    val topic = sk.topic(rooster)
    val pr    = topic.prRdd(RoosterData.rdd.map(x => NJProducerRecord(1, x)))
    val run = for {
      _ <- rooster.in(ctx).admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- pr.upload.compile.drain
      _ <- topic.fromKafka.flatMap(_.crDS.save.json(path).run(blocker))
    } yield {
      val ds = topic.load.json(path).dataset.collect().flatMap(_.value).toSet
      assert(ds == RoosterData.expected)
    }
    run.unsafeRunSync()
  }

  test("avro upload/unload") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("spark.kafka.load.avro.rooster"), Rooster.avroCodec)
    val path  = "./data/test/spark/kafka/load/avro/rooster"
    val topic = sk.topic(rooster)
    val pr    = topic.prRdd(RoosterData.rdd.map(x => NJProducerRecord(1, x)))
    val run = for {
      _ <- rooster.in(ctx).admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- pr.upload.compile.drain
      _ <- topic.fromKafka.flatMap(_.save.avro(path).run(blocker))
    } yield {
      val ds  = topic.load.avro(path).dataset.collect().flatMap(_.value).toSet
      val rdd = topic.load.rdd.avro(path).rdd.flatMap(_.value).collect().toSet
      assert(ds == RoosterData.expected)
      assert(rdd == RoosterData.expected)
    }
    run.unsafeRunSync()
  }

  test("jackson upload/unload") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("spark.kafka.load.jackson.rooster"), Rooster.avroCodec)
    val path  = "./data/test/spark/kafka/load/jackson/rooster"
    val topic = sk.topic(rooster)
    val pr    = topic.prRdd(RoosterData.rdd.map(x => NJProducerRecord(1, x)))
    val run = for {
      _ <- rooster.in(ctx).admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- pr.upload.compile.drain
      _ <- topic.fromKafka.flatMap(_.save.jackson(path).run(blocker))
    } yield {
      val ds  = topic.load.jackson(path).dataset.collect().flatMap(_.value).toSet
      val rdd = topic.load.rdd.jackson(path).rdd.flatMap(_.value).collect().toSet
      assert(ds == RoosterData.expected)
      assert(rdd == RoosterData.expected)
    }
    run.unsafeRunSync()
  }

  test("binAvro upload/unload") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("spark.kafka.load.binavro.rooster"), Rooster.avroCodec)
    val path  = "./data/test/spark/kafka/load/binavro/rooster"
    val topic = sk.topic(rooster)
    val pr    = topic.prRdd(RoosterData.rdd.map(x => NJProducerRecord(1, x)))
    val run = for {
      _ <- rooster.in(ctx).admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- pr.upload.compile.drain
      _ <- topic.fromKafka.flatMap(_.save.binAvro(path).run(blocker))
    } yield {
      val ds  = topic.load.binAvro(path).dataset.collect().flatMap(_.value).toSet
      val rdd = topic.load.rdd.binAvro(path).rdd.flatMap(_.value).collect().toSet
      assert(ds == RoosterData.expected)
      assert(rdd == RoosterData.expected)
    }
    run.unsafeRunSync()
  }

  test("object-file upload/unload") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("spark.kafka.load.obj.rooster"), Rooster.avroCodec)
    val path  = "./data/test/spark/kafka/load/obj/rooster"
    val topic = sk.topic(rooster)
    val pr    = topic.prRdd(RoosterData.rdd.map(x => NJProducerRecord(1, x)))
    val run = for {
      _ <- rooster.in(ctx).admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- pr.upload.compile.drain
      _ <- topic.fromKafka.flatMap(_.save.objectFile(path).run(blocker))
    } yield {
      val ds  = topic.load.objectFile(path).dataset.collect().flatMap(_.value).toSet
      val rdd = topic.load.rdd.objectFile(path).rdd.flatMap(_.value).collect().toSet
      assert(ds == RoosterData.expected)
      assert(rdd == RoosterData.expected)
    }
    run.unsafeRunSync()
  }
}
