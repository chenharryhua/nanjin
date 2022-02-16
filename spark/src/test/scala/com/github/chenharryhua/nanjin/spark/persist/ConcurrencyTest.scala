package com.github.chenharryhua.nanjin.spark.persist

import better.files.*
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ConcurrencyTest extends AnyFunSuite {

  def rooster =
    new DatasetAvroFileHoarder[IO, Rooster](RoosterData.bigset.repartition(2).persist(), Rooster.avroCodec.avroEncoder)
  test("avro") {
    val root = NJPath("./data/test/spark/persist/interlope/avro/rooster/")
    val run = for {
      a <- rooster.avro(root / "bzip2").bzip2.run.start
      b <- rooster.avro(root / "deflate").deflate(1).run.start
      c <- rooster.avro(root / "snappy").snappy.run.start
      d <- rooster.avro(root / "xz").xz(1).run.start
      e <- rooster.avro(root / "uncompress").run.start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
      _ <- e.join
    } yield {

      (File(root.pathStr) / "bzip2").list.toList
        .filter(_.extension().contains(".avro"))
        .map(f => assert(f.name.contains(".bzip2.data.avro")))
        .ensuring(_.nonEmpty)

      (File(root.pathStr) / "deflate").list.toList
        .filter(_.extension().contains(".avro"))
        .map(f => assert(f.name.contains(".deflate-1.data.avro")))
        .ensuring(_.nonEmpty)

      (File(root.pathStr) / "snappy").list.toList
        .filter(_.extension().contains(".avro"))
        .map(f => assert(f.name.contains(".snappy.data.avro")))
        .ensuring(_.nonEmpty)

      (File(root.pathStr) / "xz").list.toList
        .filter(_.extension().contains(".avro"))
        .map(f => assert(f.name.contains(".xz-1.data.avro")))
        .ensuring(_.nonEmpty)

      (File(root.pathStr) / "uncompress").list.toList
        .filter(_.extension().contains(".avro"))
        .map(f => assert(f.name.contains(".data.avro")))
        .ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }
  test("spark json") {
    val root = NJPath("./data/test/spark/persist/interlope/json/rooster/")
    val run = for {
      a <- rooster.json(root / "bzip2").bzip2.run.start
      b <- rooster.json(root / "deflate").deflate(1).run.start
      c <- rooster.json(root / "gzip").gzip.run.start
      d <- rooster.json(root / "uncompress").run.start
      _ <- a.join
      _ <- b.join
      _ <- d.join
      _ <- c.join
    } yield {
      (File(root.pathStr) / "bzip2").list.toList.filter(_.name.contains(".json.bz2")).ensuring(_.nonEmpty)
      (File(root.pathStr) / "deflate").list.toList.filter(_.name.contains(".json.deflate")).ensuring(_.nonEmpty)
      (File(root.pathStr) / "gzip").list.toList.filter(_.name.contains(".json.gz")).ensuring(_.nonEmpty)
      (File(root.pathStr) / "uncompress").list.toList.filter(_.extension().contains(".json")).ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("circe") {
    val root = NJPath("./data/test/spark/persist/interlope/circe/rooster/")
    val run = for {
      d <- rooster.circe(root / "bzip2").bzip2.run.start
      b <- rooster.circe(root / "deflate").deflate(1).run.start
      c <- rooster.circe(root / "gzip").gzip.run.start
      a <- rooster.circe(root / "uncompress").run.start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
    } yield {
      (File(root.pathStr) / "bzip2").list.toList.filter(_.name.contains(".circe.json.bz2")).ensuring(_.nonEmpty)
      (File(root.pathStr) / "deflate").list.toList.filter(_.name.contains(".circe.json.deflate")).ensuring(_.nonEmpty)
      (File(root.pathStr) / "gzip").list.toList.filter(_.name.contains(".circe.json.gz")).ensuring(_.nonEmpty)
      (File(root.pathStr) / "uncompress").list.toList.filter(_.name.contains(".circe.json")).ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("jackson") {
    val root = NJPath("./data/test/spark/persist/interlope/jackson/rooster/")
    val run = for {
      d <- rooster.jackson(root / "bzip2").bzip2.run.start
      b <- rooster.jackson(root / "deflate").deflate(1).run.start
      c <- rooster.jackson(root / "gzip").gzip.run.start
      a <- rooster.jackson(root / "uncompress").run.start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
    } yield {
      File(root.pathStr + "bzip2").list.toList.filter(_.name.contains(".jackson.json.bz2")).ensuring(_.nonEmpty)
      File(root.pathStr + "deflate").list.toList.filter(_.name.contains(".jackson.json.deflate")).ensuring(_.nonEmpty)
      File(root.pathStr + "gzip").list.toList.filter(_.name.contains(".jackson.json.gz")).ensuring(_.nonEmpty)
      File(root.pathStr + "uncompress").list.toList.filter(_.name.contains(".jackson.json")).ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("csv") {
    val root = NJPath("./data/test/spark/persist/interlope/csv/rooster/")
    val run = for {
      d <- rooster.csv(root / "bzip2").bzip2.run.start
      b <- rooster.csv(root / "deflate").deflate(1).run.start
      c <- rooster.csv(root / "gzip").gzip.run.start
      a <- rooster.csv(root / "uncompress").run.start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
    } yield {
      File(root.pathStr + "bzip2").list.toList.filter(_.name.contains(".csv.bz2")).ensuring(_.nonEmpty)
      File(root.pathStr + "deflate").list.toList.filter(_.name.contains(".csv.deflate")).ensuring(_.nonEmpty)
      File(root.pathStr + "gzip").list.toList.filter(_.name.contains(".csv.gz")).ensuring(_.nonEmpty)
      File(root.pathStr + "uncompress").list.toList.filter(_.name.contains(".csv")).ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("text") {
    val root = NJPath("./data/test/spark/persist/interlope/text/rooster/")
    val run = for {
      d <- rooster.text(root / "bzip2").bzip2.run.start
      b <- rooster.text(root / "deflate").deflate(1).run.start
      c <- rooster.text(root / "gzip").gzip.run.start
      a <- rooster.text(root / "uncompress").run.start
      _ <- a.join
      _ <- b.join
      _ <- c.join
      _ <- d.join
    } yield {
      File(root.pathStr + "bzip2").list.toList.filter(_.name.contains(".txt.bz2")).ensuring(_.nonEmpty)
      File(root.pathStr + "deflate").list.toList.filter(_.name.contains(".txt.deflate")).ensuring(_.nonEmpty)
      File(root.pathStr + "gzip").list.toList.filter(_.name.contains(".txt.gz")).ensuring(_.nonEmpty)
      File(root.pathStr + "uncompress").list.toList.filter(_.name.contains(".txt")).ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("parquet") {
    val root = NJPath("./data/test/spark/persist/interlope/parquet/rooster/")
    val run = for {
      d <- rooster.parquet(root / "snappy").snappy.run.start
      c <- rooster.parquet(root / "gzip").gzip.run.start
      a <- rooster.parquet(root / "uncompress").run.start
      _ <- a.join
      _ <- c.join
      _ <- d.join
    } yield {
      File(root.pathStr + "snappy").list.toList
        .filter(_.extension().contains(".parquet"))
        .map(f => assert(f.name.contains(".snappy.parquet")))
        .ensuring(_.nonEmpty)
      File(root.pathStr + "gzip").list.toList
        .filter(_.extension().contains(".parquet"))
        .map(f => assert(f.name.contains(".gz.parquet")))
        .ensuring(_.nonEmpty)
      File(root.pathStr + "uncompress").list.toList
        .filter(_.extension().contains(".parquet"))
        .map(f => assert(f.name.contains(".parquet")))
        .ensuring(_.nonEmpty)
    }
    run.unsafeRunSync()
  }

  test("mix multi") {
    val root = NJPath("./data/test/spark/persist/interlope/mix-multi/")
    val run = for {
      a <- rooster.avro(root / "avro1").bzip2.run.start
      b <- rooster.avro(root / "avro2").deflate(1).run.start
      c <- rooster.avro(root / "avro3").snappy.run.attempt.start
      d <- rooster.avro(root / "avro4").xz(2).run.start

      e <- rooster.jackson(root / "jackson1").gzip.run.start
      f <- rooster.jackson(root / "jackson2").deflate(4).run.start
      g <- rooster.jackson(root / "jackson3").bzip2.run.start

      h <- rooster.binAvro(root / "binAvro").run.start
      i <- rooster.objectFile(root / "obj").run.start

      j <- rooster.parquet(root / "parquet1").snappy.run.attempt.start
      k <- rooster.parquet(root / "parquet2").gzip.run.start

      l <- rooster.circe(root / "circe1").deflate(5).run.start
      m <- rooster.circe(root / "circe2").gzip.run.start
      n <- rooster.circe(root / "circe3").bzip2.run.start

      o <- rooster.json(root / "json1").deflate(3).run.start
      p <- rooster.json(root / "json2").bzip2.run.start
      q <- rooster.json(root / "json3").gzip.run.start

      r <- rooster.text(root / "text1").deflate(5).run.start
      s <- rooster.text(root / "text2").gzip.run.start
      t <- rooster.text(root / "text3").bzip2.run.start

      u <- rooster.csv(root / "csv1").deflate(5).run.start
      v <- rooster.csv(root / "csv2").gzip.run.start
      w <- rooster.csv(root / "csv3").bzip2.run.start

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
