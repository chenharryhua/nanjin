package mtest.spark.persist

import better.files._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ConcurrencyTest extends AnyFunSuite {

  val rooster =
    new DatasetAvroFileHoarder[IO, Rooster](
      RoosterData.bigset.dataset.repartition(2).persist(),
      Rooster.avroCodec.avroEncoder)
  test("avro") {
    val root = "./data/test/spark/persist/interlope/avro/rooster/"
    val run = for {
      a <- rooster.avro(root + "bzip2").bzip2.folder.run.start
      b <- rooster.avro(root + "deflate").deflate(1).folder.run.start
      c <- rooster.avro(root + "snappy").snappy.folder.run.start
      d <- rooster.avro(root + "xz").xz(1).folder.run.start
      e <- rooster.avro(root + "uncompress").folder.run.start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
      _ <- e.join
    } yield {

      File(root + "bzip2").list.toList
        .filter(_.extension().contains(".avro"))
        .map(f => assert(f.name.contains(".bzip2.data.avro")))
        .ensuring(_.nonEmpty)

      File(root + "deflate").list.toList
        .filter(_.extension().contains(".avro"))
        .map(f => assert(f.name.contains(".deflate-1.data.avro")))
        .ensuring(_.nonEmpty)

      File(root + "snappy").list.toList
        .filter(_.extension().contains(".avro"))
        .map(f => assert(f.name.contains(".snappy.data.avro")))
        .ensuring(_.nonEmpty)

      File(root + "xz").list.toList
        .filter(_.extension().contains(".avro"))
        .map(f => assert(f.name.contains(".xz-1.data.avro")))
        .ensuring(_.nonEmpty)

      File(root + "uncompress").list.toList
        .filter(_.extension().contains(".avro"))
        .map(f => assert(f.name.contains(".data.avro")))
        .ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }
  test("spark json") {
    val root = "./data/test/spark/persist/interlope/json/rooster/"
    val run = for {
      a <- rooster.json(root + "bzip2").bzip2.run.start
      b <- rooster.json(root + "deflate").deflate(1).run.start
      c <- rooster.json(root + "gzip").gzip.run.start
      d <- rooster.json(root + "uncompress").run.start
      _ <- a.join
      _ <- b.join
      _ <- d.join
      _ <- c.join
    } yield {
      File(root + "bzip2").list.toList.filter(_.name.contains(".json.bz2")).ensuring(_.nonEmpty)
      File(root + "deflate").list.toList.filter(_.name.contains(".json.deflate")).ensuring(_.nonEmpty)
      File(root + "gzip").list.toList.filter(_.name.contains(".json.gz")).ensuring(_.nonEmpty)
      File(root + "uncompress").list.toList.filter(_.extension().contains(".json")).ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("circe") {
    val root = "./data/test/spark/persist/interlope/circe/rooster/"
    val run = for {
      d <- rooster.circe(root + "bzip2").folder.bzip2.run.start
      b <- rooster.circe(root + "deflate").folder.deflate(1).run.start
      c <- rooster.circe(root + "gzip").folder.gzip.run.start
      a <- rooster.circe(root + "uncompress").folder.run.start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
    } yield {
      File(root + "bzip2").list.toList.filter(_.name.contains(".circe.json.bz2")).ensuring(_.nonEmpty)
      File(root + "deflate").list.toList.filter(_.name.contains(".circe.json.deflate")).ensuring(_.nonEmpty)
      File(root + "gzip").list.toList.filter(_.name.contains(".circe.json.gz")).ensuring(_.nonEmpty)
      File(root + "uncompress").list.toList.filter(_.name.contains(".circe.json")).ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("jackson") {
    val root = "./data/test/spark/persist/interlope/jackson/rooster/"
    val run = for {
      d <- rooster.jackson(root + "bzip2").folder.bzip2.run.start
      b <- rooster.jackson(root + "deflate").folder.deflate(1).run.start
      c <- rooster.jackson(root + "gzip").folder.gzip.run.start
      a <- rooster.jackson(root + "uncompress").folder.run.start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
    } yield {
      File(root + "bzip2").list.toList.filter(_.name.contains(".jackson.json.bz2")).ensuring(_.nonEmpty)
      File(root + "deflate").list.toList.filter(_.name.contains(".jackson.json.deflate")).ensuring(_.nonEmpty)
      File(root + "gzip").list.toList.filter(_.name.contains(".jackson.json.gz")).ensuring(_.nonEmpty)
      File(root + "uncompress").list.toList.filter(_.name.contains(".jackson.json")).ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("csv") {
    val root = "./data/test/spark/persist/interlope/csv/rooster/"
    val run = for {
      d <- rooster.csv(root + "bzip2").folder.bzip2.run.start
      b <- rooster.csv(root + "deflate").folder.deflate(1).run.start
      c <- rooster.csv(root + "gzip").folder.gzip.run.start
      a <- rooster.csv(root + "uncompress").folder.run.start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
    } yield {
      File(root + "bzip2").list.toList.filter(_.name.contains(".csv.bz2")).ensuring(_.nonEmpty)
      File(root + "deflate").list.toList.filter(_.name.contains(".csv.deflate")).ensuring(_.nonEmpty)
      File(root + "gzip").list.toList.filter(_.name.contains(".csv.gz")).ensuring(_.nonEmpty)
      File(root + "uncompress").list.toList.filter(_.name.contains(".csv")).ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("text") {
    val root = "./data/test/spark/persist/interlope/text/rooster/"
    val run = for {
      d <- rooster.text(root + "bzip2").folder.bzip2.run.start
      b <- rooster.text(root + "deflate").folder.deflate(1).run.start
      c <- rooster.text(root + "gzip").folder.gzip.run.start
      a <- rooster.text(root + "uncompress").folder.run.start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
    } yield {
      File(root + "bzip2").list.toList.filter(_.name.contains(".txt.bz2")).ensuring(_.nonEmpty)
      File(root + "deflate").list.toList.filter(_.name.contains(".txt.deflate")).ensuring(_.nonEmpty)
      File(root + "gzip").list.toList.filter(_.name.contains(".txt.gz")).ensuring(_.nonEmpty)
      File(root + "uncompress").list.toList.filter(_.name.contains(".txt")).ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("parquet") {
    val root = "./data/test/spark/persist/interlope/parquet/rooster/"
    val run = for {
      d <- rooster.parquet(root + "snappy").folder.snappy.run.start
      c <- rooster.parquet(root + "gzip").folder.gzip.run.start
      a <- rooster.parquet(root + "uncompress").folder.run.start
      _ <- a.join
      _ <- c.join
      _ <- d.join
    } yield {
      File(root + "snappy").list.toList
        .filter(_.extension().contains(".parquet"))
        .map(f => assert(f.name.contains(".snappy.parquet")))
        .ensuring(_.nonEmpty)
      File(root + "gzip").list.toList
        .filter(_.extension().contains(".parquet"))
        .map(f => assert(f.name.contains(".gz.parquet")))
        .ensuring(_.nonEmpty)
      File(root + "uncompress").list.toList
        .filter(_.extension().contains(".parquet"))
        .map(f => assert(f.name.contains(".parquet")))
        .ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("mix single") {
    val root = "./data/test/spark/persist/interlope/mix-single/"
    val run = for {
      a <- rooster.avro(root + "avro1.gzip2.avro").bzip2.file.run.start
      b <- rooster.avro(root + "avro2.deflate.avro").deflate(1).file.run.start
      c <- rooster.avro(root + "avro3.snapp.avro").snappy.file.run.start
      d <- rooster.avro(root + "avro4.xz.avro").xz(2).file.run.start

      e <- rooster.jackson(root + "jackson1.json.gz").file.gzip.run.start
      f <- rooster.jackson(root + "jackson2.json.deflate").file.deflate(4).run.start

      g <- rooster.binAvro(root + "binAvro.avro").file.run.start

      h <- rooster.circe(root + "circe1.json.deflate").file.deflate(5).run.start
      i <- rooster.circe(root + "circe2.json.gz").file.gzip.run.start

      j <- rooster.text(root + "text1.txt.deflate").file.deflate(5).run.start
      k <- rooster.text(root + "text2.txt.gz").file.gzip.run.start

      l <- rooster.csv(root + "csv1.csv.deflate").file.deflate(5).run.start
      m <- rooster.csv(root + "csv2.csv.gz").file.gzip.run.start

      n <- rooster.parquet(root + "parquet1.snappy.parquet").file.snappy.run.start
      o <- rooster.parquet(root + "parquet2.gz.parquet").file.gzip.run.start
      p <- rooster.parquet(root + "parquet3.uncompress.parquet").file.uncompress.run.start
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
      a <- rooster.avro(root + "avro1").bzip2.folder.run.start
      b <- rooster.avro(root + "avro2").deflate(1).folder.run.start
      c <- rooster.avro(root + "avro3").snappy.folder.run.attempt.start
      d <- rooster.avro(root + "avro4").xz(2).folder.run.start

      e <- rooster.jackson(root + "jackson1").folder.gzip.run.start
      f <- rooster.jackson(root + "jackson2").folder.deflate(4).run.start
      g <- rooster.jackson(root + "jackson3").folder.bzip2.run.start

      h <- rooster.binAvro(root + "binAvro").folder.run.start
      i <- rooster.objectFile(root + "obj").run.start

      j <- rooster.parquet(root + "parquet1").folder.snappy.run.attempt.start
      k <- rooster.parquet(root + "parquet2").folder.gzip.run.start

      l <- rooster.circe(root + "circe1").folder.deflate(5).run.start
      m <- rooster.circe(root + "circe2").folder.gzip.run.start
      n <- rooster.circe(root + "circe3").folder.bzip2.run.start

      o <- rooster.json(root + "json1").deflate(3).run.start
      p <- rooster.json(root + "json2").bzip2.run.start
      q <- rooster.json(root + "json3").gzip.run.start

      r <- rooster.text(root + "text1").folder.deflate(5).run.start
      s <- rooster.text(root + "text2").folder.gzip.run.start
      t <- rooster.text(root + "text3").folder.bzip2.run.start

      u <- rooster.csv(root + "csv1").folder.deflate(5).run.start
      v <- rooster.csv(root + "csv2").folder.gzip.run.start
      w <- rooster.csv(root + "csv3").folder.bzip2.run.start

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
      _ <- t.join
      _ <- u.join
      _ <- v.join
      _ <- w.join
    } yield ()
    run.unsafeRunSync()
  }
}
