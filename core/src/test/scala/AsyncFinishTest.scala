/*
package mccct
package test

import mccct.Async
import gears.async
import gears.async.default.given
import java.util.concurrent.atomic.AtomicReference

object MainAsyncFinish {
  @main
  def runAsync(): Unit =
    Scheduler.start(RandomWalk, false)
    val list = AtomicReference(List[Int]())
    async.Async.blocking:
      Finish {
        Async {
          Finish {
            list.updateAndGet(curr => 1 :: curr)
          }
          Finish {
            Async {
                list.updateAndGet(curr => 2 :: curr)
            }
          }
        }
      }
      Async {
        list.updateAndGet(curr => 3 :: curr)
      }

    Scheduler.awaitTermination()
    val finalList = list.get()
    assert(finalList.size == 3)
    assert(finalList == List(1, 2, 3))
}
 */
