package mccct

import gears.async.Async
import gears.async.default.given

object Main {
  @main
  def runMainEx(): Unit =
    println("Main running...")
    Scheduler.start(FifoAlgorithm)

    Async.blocking:
      Future {
        println("task 1 running...")
      }
      Future {
        println("task 2 running...")
      }
      Future {
        println("task 3 running...")
      }

    Scheduler.awaitTermination()
}
