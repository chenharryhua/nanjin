package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.kafka.{CompareDataset, KafkaMsgDigest}
import frameless.TypedDataset
import org.scalatest.funsuite.AnyFunSuite
import frameless.cats.implicits._

object InvestigationCompareDataTestData {
  final case class Mouse(size: Int, weight: Float)

  val mouses1 = List(
    KafkaMsgDigest(0, 1, "mike1".hashCode, Mouse(1, 0.1f).hashCode()),
    KafkaMsgDigest(0, 2, "mike2".hashCode, Mouse(2, 0.2f).hashCode()),
    KafkaMsgDigest(0, 3, "mike3".hashCode, Mouse(3, 0.3f).hashCode()),
    KafkaMsgDigest(1, 4, "mike4".hashCode, Mouse(4, 0.4f).hashCode()),
    KafkaMsgDigest(1, 5, "mike5".hashCode, Mouse(5, 0.5f).hashCode()),
    KafkaMsgDigest(1, 6, "mike6".hashCode, Mouse(6, 0.6f).hashCode())
  )

  val mouses2 = List( // identical to mouse1
    KafkaMsgDigest(0, 1, "mike1".hashCode, Mouse(1, 0.1f).hashCode()),
    KafkaMsgDigest(0, 2, "mike2".hashCode, Mouse(2, 0.2f).hashCode()),
    KafkaMsgDigest(0, 3, "mike3".hashCode, Mouse(3, 0.3f).hashCode()),
    KafkaMsgDigest(1, 4, "mike4".hashCode, Mouse(4, 0.4f).hashCode()),
    KafkaMsgDigest(1, 5, "mike5".hashCode, Mouse(5, 0.5f).hashCode()),
    KafkaMsgDigest(1, 6, "mike6".hashCode, Mouse(6, 0.6f).hashCode())
  )

  val mouses3 = List( // data diff (1,6) from mouse1
    KafkaMsgDigest(0, 1, "mike1".hashCode, Mouse(1, 0.1f).hashCode()),
    KafkaMsgDigest(0, 2, "mike2".hashCode, Mouse(2, 0.2f).hashCode()),
    KafkaMsgDigest(0, 3, "mike3".hashCode, Mouse(3, 0.3f).hashCode()),
    KafkaMsgDigest(1, 4, "mike4".hashCode, Mouse(4, 0.4f).hashCode()),
    KafkaMsgDigest(1, 5, "mike5".hashCode, Mouse(5, 0.5f).hashCode()),
    KafkaMsgDigest(1, 6, "mike6".hashCode, Mouse(6, 2.0f).hashCode())
  )

  val mouses4 = List( // missing (1,5) from mouse1
    KafkaMsgDigest(0, 1, "mike1".hashCode, Mouse(1, 0.1f).hashCode()),
    KafkaMsgDigest(0, 2, "mike2".hashCode, Mouse(2, 0.2f).hashCode()),
    KafkaMsgDigest(0, 3, "mike3".hashCode, Mouse(3, 0.3f).hashCode()),
    KafkaMsgDigest(1, 4, "mike4".hashCode, Mouse(4, 0.4f).hashCode()),
    KafkaMsgDigest(1, 6, "mike6".hashCode, Mouse(6, 0.6f).hashCode())
  )
}

class InvestigationCompareDataTest extends AnyFunSuite {
  import InvestigationCompareDataTestData._

  test("identical") {

    assert(
      0 === new CompareDataset(
        TypedDataset.create(mouses1).dataset,
        TypedDataset.create(mouses2).dataset).run.count[IO]().unsafeRunSync())

  }

  test("one mismatch") {

    assert(
      KafkaMsgDigest(1, 6, "mike6".hashCode, Mouse(6, 0.6f).hashCode()) === new CompareDataset(
        TypedDataset.create(mouses1).dataset,
        TypedDataset.create(mouses3).dataset).run.collect[IO]().unsafeRunSync().head)

  }

  test("one lost") {

    assert(
      KafkaMsgDigest(1, 5, "mike5".hashCode, Mouse(5, 0.5f).hashCode()) === new CompareDataset(
        TypedDataset.create(mouses1).dataset,
        TypedDataset.create(mouses4).dataset).run.collect[IO]().unsafeRunSync().head)

  }
}
