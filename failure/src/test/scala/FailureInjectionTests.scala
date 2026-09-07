import mccct._

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import java.util.concurrent.atomic.AtomicReference

import gears.async.Async
import gears.async.default.given

import java.io.File
import mccct.Scheduler.possibleFailure

@RunWith(classOf[JUnit4])
class FailureInjectionTests {

  class ExampleFailure extends Throwable

  /** Given that a schedule with failures are supplied to the scheduler, the generated schedule should be the same as
    * the one supplied.
    */
  @Test
  def replayAndRecordWithFailuresAreSynced(): Unit = {
    def testFunc(): Unit = {
      Async.blocking:
        val f = Future {
          try {
            possibleFailure(new ExampleFailure)
          } catch {
            case _: ExampleFailure => ()
          }
          val f1 = Future {
            try {
              possibleFailure(new ExampleFailure)
            } catch {
              case _: ExampleFailure => ()
            }
          }
          try {
            possibleFailure(new ExampleFailure)
          } catch {
            case _: ExampleFailure => ()
          }
          val f2 = Future {
            try {
              possibleFailure(new ExampleFailure)
            } catch {
              case _: ExampleFailure => ()
            }
          }
          try {
            possibleFailure(new ExampleFailure)
          } catch {
            case _: ExampleFailure => ()
          }
        }
    }

    val schedule = List("1.|0.1.1", "1.1.|1", "1.1.0.", "1.2.|0", "1.2.0.", "1.0.")
    Scheduler.start(alg = FixedSchedule(schedule))
    testFunc()
    Scheduler.awaitTermination()
    assert(Scheduler.getSchedule() == schedule)
  }

  /** Even if a failure is unhandled, the information should be given to the scheduler so that the program run can be
    * reproduced.
    */
  @Test
  def unhandledInjectedFailureStillIncludedInSchedule(): Unit = {
    def testFunc(): Unit = {
      Async.blocking:
        val f = Future {
          // Handled failure
          try {
            possibleFailure(new ExampleFailure)
          } catch {
            case _: ExampleFailure => ()
          }
          // Unhandled failure
          possibleFailure(new ExampleFailure)
          // Never reached
          possibleFailure(new ExampleFailure)
        }
    }

    Scheduler.start(failureAlg = AlwaysInject)
    testFunc()
    Scheduler.awaitTermination()
    // The last thing that happens in the schedule should be the unhandled failure
    assert(Scheduler.getSchedule().last == "1.|1.1")
  }

  /** If we run InjectOnNew with a bound, it should only inject that many failures.
    */
  @Test
  def boundedInjectOnNewInjectsCorrectly(): Unit = {
    def testFunc: Int = {
      var res = 0
      try {
        possibleFailure(new ExampleFailure)
      } catch {
        case _: ExampleFailure => res += 1
      }
      try {
        possibleFailure(new ExampleFailure)
      } catch {
        case _: ExampleFailure => res += 1
      }
      try {
        possibleFailure(new ExampleFailure)
      } catch {
        case _: ExampleFailure => res += 1
      }
      
      res
    }

    // Since we bound the failure injection to 1, the result should always be 1.
    def assertion(res: Int): Boolean = res == 1

    // We run the algorithm for 3 iterations, since we have 3 unique
    // failure points, 1 should be triggered at each iteration.
    Scheduler.run(testFunc, 3, assertion, failureAlg = InjectOnNew(NeverInject, 1))
  }
}
