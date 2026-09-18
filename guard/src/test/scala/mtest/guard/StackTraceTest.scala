package mtest.guard

import com.github.chenharryhua.nanjin.guard.config.StackTrace
import org.scalatest.funsuite.AnyFunSuite

class StackTraceTest extends AnyFunSuite {

  // a two-level exception so the root-cause-first ordering is observable
  private val ex: Throwable = new RuntimeException("outer", new IllegalStateException("root-cause"))
  private val st: StackTrace = StackTrace(ex)

  test("1.apply captures a non-empty, root-cause-first trace") {
    assert(st.value.nonEmpty)
    // getRootCauseStackTraceList puts the deepest cause first, so the head mentions the root cause
    assert(st.headOption.exists(_.contains("root-cause")))
  }

  test("2.topN keeps the first n frames") {
    val two = st.topN(2)
    assert(two.value == st.value.take(2))
    assert(two.value.sizeIs <= 2)
  }

  test("3.topN(1) agrees with headOption") {
    assert(st.topN(1).value == st.headOption.toList)
  }

  test("4.topN with n at or below zero yields an empty trace") {
    assert(st.topN(0).value.isEmpty)
    assert(st.topN(-5).value.isEmpty)
  }

  test("5.topN larger than the trace keeps every frame") {
    val big = st.topN(st.value.size + 100)
    assert(big.value == st.value)
  }
}
