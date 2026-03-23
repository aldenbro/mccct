package mccct.test

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import mccct.Scheduler
import mccct.Future

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
    assert(Scheduler.getNumErrors() == 3)
  }
}
