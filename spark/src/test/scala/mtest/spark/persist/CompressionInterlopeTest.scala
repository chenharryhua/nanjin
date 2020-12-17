package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{DatasetAvroFileHoarder, RddAvroFileHoarder}
import org.scalatest.funsuite.AnyFunSuite

class CompressionInterlopeTest extends AnyFunSuite {

  val rooster =
    new DatasetAvroFileHoarder[IO, Rooster](
      RoosterData.bigset.dataset,
      Rooster.avroCodec.avroEncoder)
  test("avro") {
    val root = "./data/test/spark/persist/interlope/avro/rooster/"
    val run = for {
      a <- rooster.avro(root + "bzip2").bzip2.run(blocker).start
      b <- rooster.avro(root + "deflate").deflate(1).run(blocker).start
      c <- rooster.avro(root + "snappy").snappy.run(blocker).start
      d <- rooster.avro(root + "xz").xz(1).run(blocker).start
      e <- rooster.avro(root + "uncompress").run(blocker).start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
      _ <- e.join
    } yield ()
    run.unsafeRunSync()
  }
  test("spark json") {
    val root = "./data/test/spark/persist/interlope/json/rooster/"
    val run = for {
      a <- rooster.json(root + "bzip2").bzip2.run(blocker).start
      b <- rooster.json(root + "deflate").deflate(1).run(blocker).start
      c <- rooster.json(root + "gzip").gzip.run(blocker).start
      d <- rooster.json(root + "uncompress").run(blocker).start
      _ <- a.join
      _ <- b.join
      _ <- d.join
      _ <- c.join
    } yield ()
    run.unsafeRunSync()
  }
  test("circe") {
    val root = "./data/test/spark/persist/interlope/circe/rooster/"
    val run = for {
      b <- rooster.circe(root + "deflate").deflate(1).run(blocker).start
      c <- rooster.circe(root + "gzip").gzip.run(blocker).start
      a <- rooster.circe(root + "uncompress").run(blocker).start
      _ <- a.join
      _ <- b.join
      _ <- c.join
    } yield ()
    run.unsafeRunSync()
  }
  test("mix") {
    val root = "./data/test/spark/persist/interlope/mix/rooster/"
    val run = for {
      a <- rooster.avro(root + "avro").bzip2.run(blocker).start
      b <- rooster.json(root + "json").deflate(1).run(blocker).start
      c <- rooster.jackson(root + "jackson").gzip.run(blocker).start
      d <- rooster.binAvro(root + "binAvro").run(blocker).start
      e <- rooster.parquet(root + "parquet").snappy.run(blocker).start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
      _ <- e.join
    } yield ()
    run.unsafeRunSync()
  }
}
