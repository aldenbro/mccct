package mccct
package test

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import gears.async.Async
import gears.async.default.given

import java.io.File

@RunWith(classOf[JUnit4])
class RecordTests {

  /** A test that makes sure that if the scheduler is given a fixed schedule, then the recorded schedule is the same
    * schedule
    *
    * A caveat of this test is that the FixedSchedule needs to be complete. I.E. lead to termination of the code
    */
  @Test
  def replayAndRecordTest(): Unit = {
    val schedule: List[String] = List(
      "1.",
      "2.",
      "1.2.",
      "1.2.0.",
      "2.1.",
      "1.1.",
      "2.1.0.",
      "1.1.0.",
      "1.",
      "2.2.",
      "2.2.0.",
      "1.",
      "2.",
      "1.0.",
      "2.",
      "2.0."
    )

    Scheduler.start(FixedSchedule(schedule))

    Async.blocking:
      val f1 = Future {
        val fut11 = Future {}
        val fut12 = Future {}
        fut11.await
        fut12.await
      }
      val f2 = Future {
        val fut21 = Future {}
        val fut22 = Future {}
        fut21.await
        fut22.await
      }

    Scheduler.awaitTermination()
    assert(schedule == Scheduler.getSchedule())
  }

  @Test
  def stressExistingRecordTests(): Unit = {
    var counter = 1_000
    while (counter > 0) {
      replayAndRecordTest()
      counter -= 1
    }
  }

  @Test
  def recordExceptionFuture(): Unit = {
    def testFunc() = {
      Async.blocking:
        val f1 = Future {
          throw new RuntimeException("Test runtime exception")
        }
    }
    Scheduler.start(RandomWalk)
    testFunc()
    Scheduler.awaitTermination()
    val outputFile = "test-trace.txt"
    // Since the future fails it should never be completed, i.e. no ".0." child

    assert(Scheduler.getSchedule() == List("1."))

    val (hasNoErrors, hasReliableErrors) =
      Scheduler.handleErrors(testFunc(), (), Scheduler.getSchedule(), outputFile = outputFile)
    assert(!hasNoErrors)
    assert(hasReliableErrors)
    assert(Scheduler.getSchedule() == Scheduler.readSchedule(outputFile).dropRight(1))
    val f = new File(outputFile)
    f.delete()
  }

  @Test
  def testNoExceptionFuture(): Unit = {
    def testFunc() = {
      Async.blocking:
        val f1 = Future {
          Future {}
        }
        val f2 = Future {}
    }
    Scheduler.start(RandomWalk)
    testFunc()
    Scheduler.awaitTermination()
    val outputFile                       = "test-trace.txt"
    val (hasNoErrors, hasReliableResult) =
      Scheduler.handleErrors(testFunc(), (), Scheduler.getSchedule(), outputFile = outputFile)
    assert(hasNoErrors)
    assert(hasReliableResult)
  }

  @Test
  def testMultipleExceptionFuture(): Unit = {
    def testFunc() = {
      Async.blocking:
        val f1 = Future {
          Future {
            throw new RuntimeException("Test runtime exception 1")
          }
          throw new RuntimeException("Test runtime exception 2")
        }
        val f2 = Future {}
    }

    Scheduler.start(RandomWalk)
    testFunc()
    Scheduler.awaitTermination()
    // 2 failed futures (no ".0." child), 1 successful future with 1 ".0." child
    assert(Scheduler.getSchedule().size == 4)
    val outputFile = "test-trace.txt"

    val (hasNoErrors, hasReliableErrors) =
      Scheduler.handleErrors(testFunc(), (), Scheduler.getSchedule(), outputFile = outputFile)
    assert(!hasNoErrors)
    assert(hasReliableErrors)

    assert(Scheduler.getNumErrors() == 2)
    assert(Scheduler.getSchedule() == Scheduler.readSchedule(outputFile).dropRight(1))
    val f = new File(outputFile)
    f.delete()
  }

}
