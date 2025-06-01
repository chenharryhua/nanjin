package com.github.chenharryhua.nanjin.spark.persist

import better.files.File
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import eu.timepit.refined.auto.*
import kantan.csv.CsvConfiguration
//import org.scalatest.DoNotDiscover
import io.lemonlabs.uri.typesafe.dsl.*
import org.scalatest.funsuite.AnyFunSuite

//@DoNotDiscover
class ConcurrencyTest extends AnyFunSuite {

  def rooster =
    new RddAvroFileHoarder[Rooster](RoosterData.bigset.repartition(2).persist().rdd, Rooster.avroCodec)
  test("avro") {
    val root = "./data/test/spark/persist/interlope/avro/rooster/"
    val run  = for {
      a <- rooster.avro(root / "bzip2").withCompression(_.Bzip2).run[IO].start
      b <- rooster.avro(root / "deflate").withCompression(_.Deflate(1)).run[IO].start
      c <- rooster.avro(root / "snappy").withCompression(_.Snappy).run[IO].start
      d <- rooster.avro(root / "xz").withCompression(_.Xz(1)).run[IO].start
      e <- rooster.avro(root / "uncompressed").run[IO].start
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

      File(root + "uncompressed").list.toList
        .filter(_.extension().contains(".avro"))
        .map(f => assert(f.name.contains(".data.avro")))
        .ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("circe") {
    val root = "./data/test/spark/persist/interlope/circe/rooster/"
    val run  = for {
      d <- rooster.circe(root / "bzip2").withCompression(_.Bzip2).run[IO].start
      b <- rooster.circe(root / "deflate").withCompression(_.Deflate(1)).run[IO].start
      c <- rooster.circe(root / "gzip").withCompression(_.Gzip).run[IO].start
      a <- rooster.circe(root / "uncompressed").run[IO].start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
    } yield {
      (File(root) / "bzip2").list.toList.filter(_.name.contains(".circe.json.bz2")).ensuring(_.nonEmpty)
      (File(root) / "deflate").list.toList.filter(_.name.contains(".circe.json.deflate")).ensuring(_.nonEmpty)
      (File(root) / "gzip").list.toList.filter(_.name.contains(".circe.json.gz")).ensuring(_.nonEmpty)
      (File(root) / "uncompressed").list.toList.filter(_.name.contains(".circe.json")).ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("jackson") {
    val root = "./data/test/spark/persist/interlope/jackson/rooster/"
    val run  = for {
      d <- rooster.jackson(root / "bzip2").withCompression(_.Bzip2).run[IO].start
      b <- rooster.jackson(root / "deflate").withCompression(_.Deflate(1)).run[IO].start
      c <- rooster.jackson(root / "gzip").withCompression(_.Gzip).run[IO].start
      a <- rooster.jackson(root / "uncompressed").run[IO].start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
    } yield {
      File(root + "bzip2").list.toList.filter(_.name.contains(".jackson.json.bz2")).ensuring(_.nonEmpty)
      File(root + "deflate").list.toList.filter(_.name.contains(".jackson.json.deflate")).ensuring(_.nonEmpty)
      File(root + "gzip").list.toList.filter(_.name.contains(".jackson.json.gz")).ensuring(_.nonEmpty)
      File(root + "uncompressed").list.toList.filter(_.name.contains(".jackson.json")).ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("csv") {
    val root = "./data/test/spark/persist/interlope/csv/rooster/"
    val cfg  = CsvConfiguration.rfc
    val run  = for {
      d <- rooster.kantan(root / "bzip2", cfg).withCompression(_.Bzip2).run[IO].start
      b <- rooster.kantan(root / "deflate", cfg).withCompression(_.Deflate(1)).run[IO].start
      c <- rooster.kantan(root / "gzip", cfg).withCompression(_.Gzip).run[IO].start
      a <- rooster.kantan(root / "uncompress", cfg).run[IO].start
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
    val run  = for {
      d <- rooster.text(root / "bzip2").withCompression(_.Bzip2).run[IO].start
      b <- rooster.text(root / "deflate").withCompression(_.Deflate(1)).run[IO].start
      c <- rooster.text(root / "gzip").withCompression(_.Gzip).run[IO].start
      a <- rooster.text(root / "uncompress").run[IO].start
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
    val run  = for {
      d <- rooster.parquet(root / "snappy").withCompression(_.Snappy).run[IO].start
      c <- rooster.parquet(root / "gzip").withCompression(_.Gzip).run[IO].start
      a <- rooster.parquet(root / "uncompress").run[IO].start
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

  test("mix multi") {
    val root = "./data/test/spark/persist/interlope/mix-multi/"
    val cfg  = CsvConfiguration.rfc
    val run  = for {
      a <- rooster.avro(root / "avro1").withCompression(_.Bzip2).run[IO].start
      b <- rooster.avro(root / "avro2").withCompression(_.Deflate(1)).run[IO].start
      c <- rooster.avro(root / "avro3").withCompression(_.Snappy).run[IO].attempt.start
      d <- rooster.avro(root / "avro4").withCompression(_.Xz(2)).run[IO].start

      e <- rooster.jackson(root / "jackson1").withCompression(_.Gzip).run[IO].start
      f <- rooster.jackson(root / "jackson2").withCompression(_.Deflate(4)).run[IO].start
      g <- rooster.jackson(root / "jackson3").withCompression(_.Bzip2).run[IO].start

      h <- rooster.binAvro(root / "binAvro").run[IO].start
      i <- rooster.objectFile(root / "obj").run[IO].start

      j <- rooster.parquet(root / "parquet1").withCompression(_.Snappy).run[IO].attempt.start
      k <- rooster.parquet(root / "parquet2").withCompression(_.Gzip).run[IO].start

      l <- rooster.circe(root / "circe1").withCompression(_.Deflate(5)).run[IO].start
      m <- rooster.circe(root / "circe2").withCompression(_.Gzip).run[IO].start
      n <- rooster.circe(root / "circe3").withCompression(_.Bzip2).run[IO].start

      r <- rooster.text(root / "text1").withCompression(_.Deflate(5)).run[IO].start
      s <- rooster.text(root / "text2").withCompression(_.Gzip).run[IO].start
      t <- rooster.text(root / "text3").withCompression(_.Bzip2).run[IO].start

      u <- rooster.kantan(root / "csv1", cfg).withCompression(_.Deflate(5)).run[IO].start
      v <- rooster.kantan(root / "csv2", cfg).withCompression(_.Gzip).run[IO].start
      w <- rooster.kantan(root / "csv3", cfg).withCompression(_.Bzip2).run[IO].start

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
