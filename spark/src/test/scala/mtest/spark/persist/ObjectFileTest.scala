package mtest.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.spark.persist.RddFileHoarder
import io.lemonlabs.uri.Url
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
@DoNotDiscover
class ObjectFileTest extends AnyFunSuite {
  import TabletData.*
  val hdp = sparkSession.hadoop[IO]
  test("object file identity") {
    val path = Url.parse("./data/test/spark/persist/object/tablet.obj")
    val saver = new RddFileHoarder[Tablet](rdd).objectFile(path)
    saver.run[IO].unsafeRunSync()
    val t = sparkSession.loadRdd[Tablet](path).objectFile.collect().toSet
    assert(data.toSet == t)
//    val t2 = Stream
//      .force(hdp.filesByName(path).map {
//        _.foldLeft(Stream.empty.covaryAll[IO, Tablet]) { case (ss, i) =>
//          ss ++ hdp.bytes.source(i).through(JavaObjectSerde.fromBytes[IO, Tablet])
//        }
//      })
//      .compile
//      .toList
//      .map(_.toSet)
//    assert(t2.unsafeRunSync() == t)
  }
}
