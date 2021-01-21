package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import mtest.spark._

@DoNotDiscover
class CompressionInterlopeTest extends AnyFunSuite {

  val rooster =
    new DatasetAvroFileHoarder[IO, Rooster](
      RoosterData.bigset.dataset.repartition(2).persist(),
      Rooster.avroCodec.avroEncoder)
  test("avro") {
    val root = "./data/test/spark/persist/interlope/avro/rooster/"
    val run = for {
      a <- rooster.avro(root + "bzip2").bzip2.folder.run(blocker).start
      b <- rooster.avro(root + "deflate").deflate(1).folder.run(blocker).start
      c <- rooster.avro(root + "snappy").snappy.folder.run(blocker).start
      d <- rooster.avro(root + "xz").xz(1).folder.run(blocker).start
      e <- rooster.avro(root + "uncompress").folder.run(blocker).start
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
      b <- rooster.circe(root + "deflate").folder.deflate(1).run(blocker).start
      c <- rooster.circe(root + "gzip").folder.gzip.run(blocker).start
      a <- rooster.circe(root + "uncompress").folder.run(blocker).start
      _ <- a.join
      _ <- b.join
      _ <- c.join
    } yield ()
    run.unsafeRunSync()
  }
  test("mix single") {
    val root = "./data/test/spark/persist/interlope/mix-single/"
    val run = for {
      a <- rooster.avro(root + "avro1.gzip2.avro").bzip2.file.run(blocker).start
      b <- rooster.avro(root + "avro2.deflate.avro").deflate(1).file.run(blocker).start
      c <- rooster.avro(root + "avro3.snapp.avro").snappy.file.run(blocker).start
      d <- rooster.avro(root + "avro4.xz.avro").xz(2).file.run(blocker).start

      e <- rooster.jackson(root + "jackson1.json.gz").file.gzip.run(blocker).start
      f <- rooster.jackson(root + "jackson2.json.deflate").file.deflate(4).run(blocker).start

      g <- rooster.binAvro(root + "binAvro.avro").file.run(blocker).start

      h <- rooster.circe(root + "circe1.json.deflate").file.deflate(5).run(blocker).start
      i <- rooster.circe(root + "circe2.json.gz").file.gzip.run(blocker).start

      j <- rooster.text(root + "text1.txt.deflate").file.deflate(5).run(blocker).start
      k <- rooster.text(root + "text2.txt.gz").file.gzip.run(blocker).start

      l <- rooster.csv(root + "csv1.csv.deflate").file.deflate(5).run(blocker).start
      m <- rooster.csv(root + "csv2.csv.gz").file.gzip.run(blocker).start

      n <- rooster.parquet(root + "parquet1.snappy.parquet").file.snappy.run(blocker).start
      o <- rooster.parquet(root + "parquet2.gz.parquet").file.gzip.run(blocker).start
      p <- rooster.parquet(root + "parquet3.uncompress.parquet").file.uncompress.run(blocker).start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
      _ <- e.join
      _ <- f.join
      _ <- g.join
      _ <- h.join
      _ <- i.join
      _ <- j.join
      _ <- k.join
      _ <- l.join
      _ <- m.join
      _ <- n.join
      _ <- o.join
      _ <- p.join
    } yield ()
    run.unsafeRunSync()
  }
  test("mix multi") {
    val root = "./data/test/spark/persist/interlope/mix-multi/"
    val run = for {
      a <- rooster.avro(root + "avro1").bzip2.folder.run(blocker).start
      b <- rooster.avro(root + "avro2").deflate(1).folder.run(blocker).start
      c <- rooster.avro(root + "avro3").snappy.folder.run(blocker).attempt.start
      d <- rooster.avro(root + "avro4").xz(2).folder.run(blocker).start

      e <- rooster.jackson(root + "jackson1").gzip.run(blocker).start
      f <- rooster.jackson(root + "jackson2").deflate(4).run(blocker).start

      g <- rooster.binAvro(root + "binAvro").folder.run(blocker).start

      h <- rooster.parquet(root + "parquet1").folder.snappy.run(blocker).attempt.start
      i <- rooster.parquet(root + "parquet2").folder.gzip.run(blocker).start

      j <- rooster.circe(root + "circe1").folder.deflate(5).run(blocker).start
      k <- rooster.circe(root + "circe2").folder.gzip.run(blocker).start

      l <- rooster.json(root + "json1").gzip.run(blocker).start
      m <- rooster.json(root + "json2").deflate(3).run(blocker).start
      n <- rooster.json(root + "json3").bzip2.run(blocker).start

      o <- rooster.text(root + "text1").folder.deflate(5).run(blocker).start
      p <- rooster.text(root + "text2").folder.gzip.run(blocker).start

      r <- rooster.csv(root + "csv1").deflate(5).run(blocker).start
      s <- rooster.csv(root + "csv2").gzip.run(blocker).start

      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
      _ <- e.join
      _ <- f.join
      _ <- g.join
      _ <- h.join
      _ <- i.join
      _ <- j.join
      _ <- k.join
      _ <- l.join
      _ <- m.join
      _ <- n.join
      _ <- o.join
      _ <- p.join
      _ <- r.join
      _ <- s.join
    } yield ()
    run.unsafeRunSync()
  }
}
