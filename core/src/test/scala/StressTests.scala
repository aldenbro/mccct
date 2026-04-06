package mccct
package test

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import java.util.concurrent.atomic.AtomicInteger

import gears.async.Async
import gears.async.default.given

@RunWith(classOf[JUnit4])
class StressTests {

  @Test
  def atomicCounterFifoStressTest(): Unit = {
    Scheduler.start(FifoAlgorithm)

    val numFutures = 1_000
    var counter    = AtomicInteger(0)
    Async.blocking:
      val futures = for { _ <- 1 to numFutures } yield Future { counter.getAndIncrement() }

    Scheduler.awaitTermination()
    val scheduleNonChild = Scheduler.getSchedule().filter(x => !x.contains(".0.")).map(x => x.dropRight(1).toInt)
    assert(counter.get() == numFutures)                    // Is the counter correctly implemented
    assert(Scheduler.getSchedule().size == 2 * numFutures) // Did all futures start and finish?
    assert(scheduleNonChild.sliding(2).forall {            // Did we correctly use the FIFO algorithm?
      case List(a, b) => b == a + 1
      case _          => true
    })
  }

  @Test
  def atomicCounterRandomStressTest(): Unit = {
    Scheduler.start(RandomWalk)

    val numFutures = 1_000
    var counter    = AtomicInteger(0)
    Async.blocking:
      val futures = for { _ <- 1 to numFutures } yield Future { counter.getAndIncrement() }

    Scheduler.awaitTermination()
    assert(counter.get() == numFutures)                    // Is the counter correctly implemented
    assert(Scheduler.getSchedule().size == 2 * numFutures) // Did all futures start and finish?
  }

}
