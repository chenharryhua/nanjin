package com.github.chenharryhua.nanjin.guard.observers

import com.github.chenharryhua.nanjin.guard.config.StackTrace
import com.github.chenharryhua.nanjin.guard.event.Event.{ReportedEvent, ServicePanic}
import mtest.guard.gendata.{reportedEvent, servicePanic, serviceStart}
import munit.FunSuite

class ObserverSharedTest extends FunSuite {

  private def stack_trace: StackTrace = {
    def deepest(): StackTrace = StackTrace(new RuntimeException("observer-stack-marker"))
    def middle(): StackTrace = deepest()
    middle()
  }

  test("1.ServicePanic stack trace is truncated to the requested number of frames") {
    val trace = stack_trace
    val original = servicePanic.copy(stackTrace = trace)

    limitStackTraceFrames(original, Some(2)) match {
      case limited: ServicePanic =>
        assertEquals(limited.stackTrace.value, trace.value.take(2))
        assertEquals(original.stackTrace, trace)
      case other => fail(s"expected ServicePanic, got $other")
    }
  }

  test("2.zero and negative limits produce an empty ServicePanic stack trace") {
    val original = servicePanic.copy(stackTrace = stack_trace)

    List(0, -1).foreach { limit =>
      limitStackTraceFrames(original, Some(limit)) match {
        case limited: ServicePanic => assertEquals(limited.stackTrace.value, Nil)
        case other                 => fail(s"expected ServicePanic, got $other")
      }
    }
  }

  test("3.a limit larger than the panic trace preserves every frame") {
    val trace = stack_trace
    val original = servicePanic.copy(stackTrace = trace)

    limitStackTraceFrames(original, Some(trace.value.size + 10)) match {
      case limited: ServicePanic => assertEquals(limited.stackTrace, trace)
      case other                 => fail(s"expected ServicePanic, got $other")
    }
  }

  test("4.None leaves a ServicePanic stack trace unchanged") {
    val original = servicePanic.copy(stackTrace = stack_trace)
    assertEquals(limitStackTraceFrames(original, None), original)
  }

  test("5.ReportedEvent optional stack trace is truncated without mutating the original") {
    val trace = stack_trace
    val original = reportedEvent.copy(logRecord = reportedEvent.logRecord.copy(stackTrace = Some(trace)))

    limitStackTraceFrames(original, Some(1)) match {
      case limited: ReportedEvent =>
        assertEquals(limited.logRecord.stackTrace.map(_.value), Some(trace.value.take(1)))
        assertEquals(original.logRecord.stackTrace, Some(trace))
      case other => fail(s"expected ReportedEvent, got $other")
    }
  }

  test("6.ReportedEvent without a stack trace remains without one") {
    val original = reportedEvent.copy(logRecord = reportedEvent.logRecord.copy(stackTrace = None))

    limitStackTraceFrames(original, Some(2)) match {
      case limited: ReportedEvent => assertEquals(limited.logRecord.stackTrace, None)
      case other                  => fail(s"expected ReportedEvent, got $other")
    }
  }

  test("7.non-stack-bearing events pass through unchanged") {
    assertEquals(limitStackTraceFrames(serviceStart, Some(1)), serviceStart)
    assertEquals(limitStackTraceFrames(serviceStart, None), serviceStart)
  }
}
