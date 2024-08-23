package com.github.chenharryhua.nanjin.guard.service

import io.circe.generic.JsonCodec

import java.time.Duration

@JsonCodec
final private[service] case class ClassLoadGauge(loaded: Int, unloaded: Long, total: Long)

@JsonCodec
final case class ThreadDeadlocks(
  thread: String,
  lock: Option[String],
  owner: Option[String],
  stack: List[String])

@JsonCodec
final private[service] case class HeapMemory(init: Long, used: Long, max: Long, committed: Long)

@JsonCodec
final private[service] case class NonHeapMemory(init: Long, used: Long, max: Long, committed: Long)

@JsonCodec
final private[service] case class GarbageCollector(name: String, count: Long, took: Duration)

@JsonCodec
final private[service] case class ThreadState(live: Int, daemon: Int, peak: Int, started: Long)

@JsonCodec
final private[service] case class OperationSystem(
  architecture: String,
  available_processors: Int,
  name: String,
  version: String,
  system_load_average: Double)

@JsonCodec
final private[service] case class AllJvmGauge(
  classloader: ClassLoadGauge,
  deadlocks: List[ThreadDeadlocks],
  garbage_collectors: List[GarbageCollector],
  heap_memory: HeapMemory,
  non_heap_memory: NonHeapMemory,
  thread_state: ThreadState,
  operation_system: OperationSystem
)
