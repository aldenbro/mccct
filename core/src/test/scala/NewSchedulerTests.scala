package mccct.test

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import mccct._
import mccct.Scheduler.checkSuspend

@RunWith(classOf[JUnit4])
class NewSchedulerTests() {

  /** The scheduler should handle throwing failures at any point in the code. Previously you could not throw errors
    * outside of concurrent objects.
    */
  @Test
  def failuresAreCountedCorrectly(): Unit = {
    Scheduler() {
      Future {
        Future {
          throw new RuntimeException("This should increment the number of errors")
        }
        throw new RuntimeException("This should also increment")
      }
      throw new RuntimeException("And so should this")

      // The following code should not run as an exception has been thrown above
      Future {
        throw new RuntimeException("This should never be thrown")
      }
    }
    assert(Scheduler.getNumErrors() == 3, s"Expected: 3, Actually: ${Scheduler.getNumErrors()}")
  }

  /** If tasks are being scheduled randomly, we should expect the number of increases (e.g. future 2. to come after 1.)
    * to follow a specific pattern.
    */
  @Test
  def innerTasksAreScheduledRandomly(): Unit = {
    val n                 = 100           // The number of futures created per iteration
    val m                 = 10            // Number of iterations
    val zCritical         = 3.291         // 99.9% confidence
    val expected          = (n - 1) / 2.0 // Expected number of increases for a random distribution
    val variance          = (n + 1) / 12.0
    val stdErr            = math.sqrt(variance / m)
    def runIteration: Int = {
      Scheduler(alg = RandomWalk, includeTaskEndings = false) {
        Future {
          // We create n inner futures that should be scheduled randomly
          (1 to n).foreach(_ => Future {})
        }
      }
      val schedule = Scheduler
        .getSchedule()                       // Get the schedule produced by the execution
        .filter(s => s != "1." && s != "0.") // Ignore the root task and the outer future
        .map(_.filter(_.isDigit).toInt)      // Turn the strings into ints (e.g. "1.12." -> 112)
      // Calculate and return the number of increases
      schedule.sliding(2).count {
        case Seq(a, b) => b > a
        case _         => false
      }
    }
    val mean = (1 to m).map(_ => runIteration).sum / m
    val z    = (mean - expected) / stdErr
    // Assert that the scheduling looks random
    assert(zCritical > math.abs(z))
  }

  /** If McCCT is run with a deterministic algorithm such as FIFO and is run sequentially, then McCCT should behave
    * deterministically as well.
    *
    * If we do not run sequentially, we do not have this guarantee, as tasks running in parallel may finish and submit
    * associated tasks at different points.
    */
  @Test
  def deterministicAlgAndSequentialExecutionGiveSameSchedules(): Unit = {
    def funcWithCheckSuspend: List[String] = {
      // Run scheduler
      Scheduler(FifoAlgorithm, sequential = true) {
        checkSuspend()
        Future {
          checkSuspend()
          Future {
            checkSuspend()
            Future {
              Future {
                Future {}
                checkSuspend()
                Future {}
              }
            }
          }
          Future {}
          checkSuspend()
        }
        checkSuspend()
        checkSuspend()
      }
      // Return produced schedule
      Scheduler.getSchedule()
    }

    println("Running check suspend function")
    checkDeterminism(funcWithCheckSuspend, 10)

    def funcWithAwait: List[String] = {
      // Run scheduler
      Scheduler(FifoAlgorithm, sequential = true) {
        val f = Future {
          Future {}
        }
        f.await
      }
      // Return produced schedule
      Scheduler.getSchedule()
    }

    // ! This will not work, since non-determinism is introduced, the controller awaiting can resubmit after or before the completed future's children are submitted
    // println("Running await function")
    // checkDeterminism(funcWithAwait, 100)
  }

  // Runs the program n times and check that every schedule produced is the same
    def checkDeterminism(func: => List[String], n: Int) = {
      var head: Option[List[String]] = None
      (1 to n).foreach(_ =>
        head match {
          case Some(schedule) =>
            val res = func
            assert(schedule == res, s"\nExpected: $schedule\nActually: $res")
          case None => head = Some(func)
        }
      )
    }
}
