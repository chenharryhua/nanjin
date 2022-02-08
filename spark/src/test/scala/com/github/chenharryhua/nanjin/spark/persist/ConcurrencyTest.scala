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

  def rooster(path: NJPath) =
    new DatasetAvroFileHoarder[IO, Rooster](
      RoosterData.bigset.repartition(2).persist(),
      Rooster.avroCodec.avroEncoder,
      HoarderConfig(path))
  test("avro") {
    val root = NJPath("./data/test/spark/persist/interlope/avro/rooster/")
    val run = for {
      a <- rooster(root / "bzip2").avro.bzip2.run.start
      b <- rooster(root / "deflate").avro.deflate(1).run.start
      c <- rooster(root / "snappy").avro.snappy.run.start
      d <- rooster(root / "xz").avro.xz(1).run.start
      e <- rooster(root / "uncompress").avro.run.start
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
      a <- rooster(root / "bzip2").json.bzip2.run.start
      b <- rooster(root / "deflate").json.deflate(1).run.start
      c <- rooster(root / "gzip").json.gzip.run.start
      d <- rooster(root / "uncompress").json.run.start
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
      d <- rooster(root / "bzip2").circe.bzip2.run.start
      b <- rooster(root / "deflate").circe.deflate(1).run.start
      c <- rooster(root / "gzip").circe.gzip.run.start
      a <- rooster(root / "uncompress").circe.run.start
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
      d <- rooster(root / "bzip2").jackson.bzip2.run.start
      b <- rooster(root / "deflate").jackson.deflate(1).run.start
      c <- rooster(root / "gzip").jackson.gzip.run.start
      a <- rooster(root / "uncompress").jackson.run.start
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
      d <- rooster(root / "bzip2").csv.bzip2.run.start
      b <- rooster(root / "deflate").csv.deflate(1).run.start
      c <- rooster(root / "gzip").csv.gzip.run.start
      a <- rooster(root / "uncompress").csv.run.start
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
      d <- rooster(root / "bzip2").text.bzip2.run.start
      b <- rooster(root / "deflate").text.deflate(1).run.start
      c <- rooster(root / "gzip").text.gzip.run.start
      a <- rooster(root / "uncompress").text.run.start
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
      d <- rooster(root / "snappy").parquet.snappy.run.start
      c <- rooster(root / "gzip").parquet.gzip.run.start
      a <- rooster(root / "uncompress").parquet.run.start
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
      a <- rooster(root / "avro1").avro.bzip2.run.start
      b <- rooster(root / "avro2").avro.deflate(1).run.start
      c <- rooster(root / "avro3").avro.snappy.run.attempt.start
      d <- rooster(root / "avro4").avro.xz(2).run.start

      e <- rooster(root / "jackson1").jackson.gzip.run.start
      f <- rooster(root / "jackson2").jackson.deflate(4).run.start
      g <- rooster(root / "jackson3").jackson.bzip2.run.start

      h <- rooster(root / "binAvro").binAvro.run.start
      i <- rooster(root / "obj").objectFile.run.start

      j <- rooster(root / "parquet1").parquet.snappy.run.attempt.start
      k <- rooster(root / "parquet2").parquet.gzip.run.start

      l <- rooster(root / "circe1").circe.deflate(5).run.start
      m <- rooster(root / "circe2").circe.gzip.run.start
      n <- rooster(root / "circe3").circe.bzip2.run.start

      o <- rooster(root / "json1").json.deflate(3).run.start
      p <- rooster(root / "json2").json.bzip2.run.start
      q <- rooster(root / "json3").json.gzip.run.start

      r <- rooster(root / "text1").text.deflate(5).run.start
      s <- rooster(root / "text2").text.gzip.run.start
      t <- rooster(root / "text3").text.bzip2.run.start

      u <- rooster(root / "csv1").csv.deflate(5).run.start
      v <- rooster(root / "csv2").csv.gzip.run.start
      w <- rooster(root / "csv3").csv.bzip2.run.start

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
