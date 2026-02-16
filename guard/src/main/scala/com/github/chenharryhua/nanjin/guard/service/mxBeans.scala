package com.github.chenharryhua.nanjin.guard.service

import cats.Eval
import cats.syntax.traverse.toTraverseOps

import java.lang.management.*
import scala.concurrent.duration.DurationLong
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.DurationConverters.ScalaDurationOps

private object mxBeans {
  private val classloader: Eval[ClassLoadGauge] =
    Eval.always {
      val mxBean: ClassLoadingMXBean = ManagementFactory.getClassLoadingMXBean
      ClassLoadGauge(
        loaded = mxBean.getLoadedClassCount,
        unloaded = mxBean.getUnloadedClassCount,
        total = mxBean.getTotalLoadedClassCount)
    }

  private val deadlocks: Eval[List[ThreadDeadlocks]] =
    Eval.always {
      val mxBean: ThreadMXBean = ManagementFactory.getThreadMXBean
      Option(mxBean.findDeadlockedThreads()).traverse {
        _.toList.map { id =>
          val info: ThreadInfo = mxBean.getThreadInfo(id, 100)
          ThreadDeadlocks(
            thread = info.getThreadName,
            lock = Option(info.getLockName),
            owner = Option(info.getLockOwnerName),
            stack = info.getStackTrace.map(_.toString).toList
          )
        }
      }.flatten
    }

  private val heapMemory: Eval[HeapMemory] = Eval.always {
    val mxBean: MemoryMXBean = ManagementFactory.getMemoryMXBean
    HeapMemory(
      init = mxBean.getHeapMemoryUsage.getInit,
      used = mxBean.getHeapMemoryUsage.getUsed,
      max = mxBean.getHeapMemoryUsage.getMax,
      committed = mxBean.getHeapMemoryUsage.getCommitted
    )
  }

  private val nonHeapMemory: Eval[NonHeapMemory] = Eval.always {
    val mxBean: MemoryMXBean = ManagementFactory.getMemoryMXBean
    NonHeapMemory(
      init = mxBean.getNonHeapMemoryUsage.getInit,
      used = mxBean.getNonHeapMemoryUsage.getUsed,
      max = mxBean.getNonHeapMemoryUsage.getMax,
      committed = mxBean.getNonHeapMemoryUsage.getCommitted
    )
  }

  private val garbageCollectors: Eval[List[GarbageCollector]] = Eval.always(
    ManagementFactory.getGarbageCollectorMXBeans.asScala.toList.map(gc =>
      GarbageCollector(
        name = gc.getName,
        count = gc.getCollectionCount,
        took = gc.getCollectionTime.milliseconds.toJava)))

  private val threadState: Eval[ThreadState] = Eval.always {
    val mxBean: ThreadMXBean = ManagementFactory.getThreadMXBean
    ThreadState(
      live = mxBean.getThreadCount,
      daemon = mxBean.getDaemonThreadCount,
      peak = mxBean.getPeakThreadCount,
      started = mxBean.getTotalStartedThreadCount
    )
  }

  private val operatingSystem: Eval[OperatingSystem] = Eval.always {
    val mxBean: OperatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean
    OperatingSystem(
      architecture = s"${mxBean.getArch} ${mxBean.getName} ${mxBean.getVersion}",
      available_processors = mxBean.getAvailableProcessors
    )
  }

  private val runtime = Eval.always {
    val mxBean = ManagementFactory.getRuntimeMXBean
    RuntimeMX(
      pid = mxBean.getPid,
      virtual_machine = s"${mxBean.getVmVendor} ${mxBean.getVmName} ${mxBean.getVmVersion}",
      input_arguments = mxBean.getInputArguments.asScala.toList
    )
  }

  val allJvmGauge: Eval[AllJvmGauge] =
    for {
      rt <- runtime
      cl <- classloader
      dl <- deadlocks
      gc <- garbageCollectors
      hp <- heapMemory
      nh <- nonHeapMemory
      ts <- threadState
      os <- operatingSystem
    } yield AllJvmGauge(
      operating_system = os,
      runtime = rt,
      classloader = cl,
      deadlocks = dl,
      garbage_collectors = gc,
      heap_memory = hp,
      non_heap_memory = nh,
      thread_state = ts
    )
}
