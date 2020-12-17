package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class CompressionInterlopeTest extends AnyFunSuite {

  val rooster =
    new DatasetAvroFileHoarder[IO, Rooster](
      RoosterData.bigset.dataset.repartition(2).persist(),
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
  test("mix single") {
    val root = "./data/test/spark/persist/interlope/mix-single/"
    val run = for {
      a <- rooster.avro(root + "avro1.gzip2.avro").file.bzip2.run(blocker).start
      b <- rooster.avro(root + "avro2.deflate.avro").file.deflate(1).run(blocker).start
      c <- rooster.avro(root + "avro3.snapp.avro").file.snappy.run(blocker).start
      d <- rooster.avro(root + "avro4.xz.avro").file.xz(2).run(blocker).start

      e <- rooster.jackson(root + "jackson1.json.gz").file.gzip.run(blocker).start
      f <- rooster.jackson(root + "jackson2.json.deflate").file.deflate(4).run(blocker).start

      g <- rooster.binAvro(root + "binAvro.avro").file.run(blocker).start

      j <- rooster.circe(root + "circe1.json.deflate").file.deflate(5).run(blocker).start
      k <- rooster.circe(root + "circe2.json.gz").file.gzip.run(blocker).start

      o <- rooster.text(root + "text1.txt.deflate").file.deflate(5).run(blocker).start
      p <- rooster.text(root + "text2.txt.gz").file.gzip.run(blocker).start

      r <- rooster.csv(root + "csv1.csv.deflate").file.deflate(5).run(blocker).start
      s <- rooster.csv(root + "csv2.csv.gz").file.gzip.run(blocker).start

      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
      _ <- e.join
      _ <- f.join
      _ <- g.join
      _ <- j.join
      _ <- k.join
      _ <- o.join
      _ <- p.join
      _ <- r.join
      _ <- s.join

    } yield ()
    run.unsafeRunSync()
  }
  test("mix multi") {
    val root = "./data/test/spark/persist/interlope/mix-multi/"
    val run = for {
      a <- rooster.avro(root + "avro1").bzip2.run(blocker).start
      b <- rooster.avro(root + "avro2").deflate(1).run(blocker).start
      c <- rooster.avro(root + "avro3").snappy.run(blocker).start
      d <- rooster.avro(root + "avro4").xz(2).run(blocker).start

      e <- rooster.jackson(root + "jackson1").gzip.run(blocker).start
      f <- rooster.jackson(root + "jackson2").deflate(4).run(blocker).start

      g <- rooster.binAvro(root + "binAvro").run(blocker).start

      h <- rooster.parquet(root + "parquet1").snappy.run(blocker).start
      i <- rooster.parquet(root + "parquet2").gzip.run(blocker).start

      j <- rooster.circe(root + "circe1").deflate(5).run(blocker).start
      k <- rooster.circe(root + "circe2").gzip.run(blocker).start

      l <- rooster.json(root + "json1").gzip.run(blocker).start
      m <- rooster.json(root + "json2").deflate(3).run(blocker).start
      n <- rooster.json(root + "json3").bzip2.run(blocker).start

      o <- rooster.text(root + "text1").deflate(5).run(blocker).start
      p <- rooster.text(root + "text2").gzip.run(blocker).start

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
