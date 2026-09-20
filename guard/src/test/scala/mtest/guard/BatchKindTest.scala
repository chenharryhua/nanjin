package mtest.guard

import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.batch.BatchKind
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.FunSuite

/** `BatchKind` is a two-case enum whose `Encoder` and `Show` both render the case name (`productPrefix`).
  * Both renders are wire contracts: the `Encoder` feeds the batch JSON reports, and `Show` feeds the batch
  * report key built in `data.batchEntry` (e.g. `show"$mode $k Batch"` -> "Sequential Quasi Batch"). These
  * tests pin the exact strings so a rename of a case cannot silently change the wire format.
  */
class BatchKindTest extends FunSuite {

  test("1.Encoder renders each case to its case name") {
    assert(BatchKind.Quasi.asJson == Json.fromString("Quasi"))
    assert(BatchKind.Value.asJson == Json.fromString("Value"))
  }

  test("2.Show renders each case to its case name") {
    assert(BatchKind.Quasi.show == "Quasi")
    assert(BatchKind.Value.show == "Value")
  }

  test("3.Encoder and Show agree for every case") {
    // guards against a future case being added without a pinned wire string: every value must encode to a
    // JSON string equal to its Show output.
    BatchKind.values.foreach { k =>
      assert(k.asJson == Json.fromString(k.show))
    }
  }
}
