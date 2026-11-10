package com.github.chenharryhua.nanjin.guard.service.dashboard

import java.time.Duration
import io.circe.Codec

/*
 * The snake_case field names below are deliberate and are NOT a lapse from the camelCase convention for
 * non-local identifiers: every one of these case classes derives `Codec.AsObject`, so each field name IS
 * the JSON key served by `GET /metrics/jvm`. Renaming a field to camelCase is a wire-format break for any
 * consumer of that endpoint, not a wire-safe rename.
 */

final private case class ClassLoadGauge(loaded: Int, unloaded: Long, total: Long) derives Codec.AsObject

final private case class ThreadDeadlocks(
  thread: String,
  lock: Option[String],
  owner: Option[String],
  stack: List[String])
    derives Codec.AsObject

final private case class HeapMemory(init: Long, used: Long, max: Long, committed: Long) derives Codec.AsObject

final private case class NonHeapMemory(init: Long, used: Long, max: Long, committed: Long)
    derives Codec.AsObject

final private case class GarbageCollector(name: String, count: Long, took: Duration) derives Codec.AsObject

final private case class ThreadState(live: Int, daemon: Int, peak: Int, started: Long) derives Codec.AsObject

final private case class OperatingSystem(architecture: String, available_processors: Int)
    derives Codec.AsObject

final private case class RuntimeMX(
  pid: Long,
  virtual_machine: String,
  input_arguments: List[String]
) derives Codec.AsObject

final private case class AllJvmGauge(
  operating_system: OperatingSystem,
  runtime: RuntimeMX,
  classloader: ClassLoadGauge,
  deadlocks: List[ThreadDeadlocks],
  garbage_collectors: List[GarbageCollector],
  heap_memory: HeapMemory,
  non_heap_memory: NonHeapMemory,
  thread_state: ThreadState
) derives Codec.AsObject
