package com.github.chenharryhua.nanjin.guard.service.dashboard


import java.time.Duration
import io.circe.Codec

final private[dashboard] case class ClassLoadGauge(loaded: Int, unloaded: Long, total: Long) derives Codec.AsObject

final case class ThreadDeadlocks(
  thread: String,
  lock: Option[String],
  owner: Option[String],
  stack: List[String]) derives Codec.AsObject

final private[dashboard] case class HeapMemory(init: Long, used: Long, max: Long, committed: Long) derives Codec.AsObject

final private[dashboard] case class NonHeapMemory(init: Long, used: Long, max: Long, committed: Long) derives Codec.AsObject

final private[dashboard] case class GarbageCollector(name: String, count: Long, took: Duration) derives Codec.AsObject

final private[dashboard] case class ThreadState(live: Int, daemon: Int, peak: Int, started: Long) derives Codec.AsObject

final private[dashboard] case class OperatingSystem(architecture: String, available_processors: Int) derives Codec.AsObject

final private[dashboard] case class RuntimeMX(
  pid: Long,
  virtual_machine: String,
  input_arguments: List[String]
) derives Codec.AsObject

final private[dashboard] case class AllJvmGauge(
  operating_system: OperatingSystem,
  runtime: RuntimeMX,
  classloader: ClassLoadGauge,
  deadlocks: List[ThreadDeadlocks],
  garbage_collectors: List[GarbageCollector],
  heap_memory: HeapMemory,
  non_heap_memory: NonHeapMemory,
  thread_state: ThreadState
) derives Codec.AsObject
