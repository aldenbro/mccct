/*
package mccct

import scala.util.control.NonFatal
import scala.util.{Try, Success, Failure}
import gears.async
import gears.async.{Cancellable, Scheduler}
import gears.async.default.given

object Finish {
  def apply[T](body: Controller ?=> T)(using a: async.Async, parent: Controller): Unit =
    val p              = async.Future.Promise[Integer]()
    val taskController = new Controller(parent, controllerType = ControllerType.Finish)
    val task: Runnable = new Runnable() {
      def run() =
        try
          // We must make a choice here if root
          if (parent.isRoot) {
            Scheduler.stuckSignal(parent)
          }
          val result = body(using taskController)
          if (taskController.totalChildren.get() != 0) {
            taskController.await() // Wait for child to signal this
          }
          // Signal the scheduler that this function has finished. This will decrement the cnt by one and possibly terminate the scheduler
          Scheduler.noLongerStuck(parent)
          p.complete(Success(1))
        catch // If an error is encountered then notify the scheduler of this
          case NonFatal(e) =>
            // Call the throwError method, which increments the number of exceptions and finishes this task
            Scheduler.throwError(e, taskController)
            p.complete(Failure(e))
          case e =>
            Scheduler.throwError(e, taskController)
            p.complete(Failure(e))
    }
    // Start task on virtual thread
    Scheduler.startThread(task, taskController)
    val result = p.asFuture.await

}

object Async {

  private def submitChild(parent: Controller)(using a: async.Async): Unit = {
    val childController = new Controller(parent, isEnd = true)
    val endChild        = new Runnable() { // The task that is executed on a new thread
      def run() = {
        try
          childController.await()           // Wait for scheduler to signal the controller to execute
          Scheduler.finish(childController) // Do nothing and finish()
        catch
          case e =>
            Scheduler.throwError(e, childController)
      }
    }

    Scheduler.startThread(endChild, childController) // Start this .0. child task on a new virtual thread
    Scheduler.submit(childController)                // Submit the child task to the scheduler
  }

  def apply[T](body: Controller ?=> T)(using a: async.Async, parent: Controller): Unit =
    val taskController = new Controller(parent)
    val task: Runnable = new Runnable() {
      def run() =
        try

          taskController.await() // Wait for scheduler to let the task start
          // Using a promise is enough, since a task is started only once
          // Try to execute the body
          val result        = body(using taskController)
          val closestFinish = taskController.closestType(ControllerType.Finish)
          // Schedule the end child before completing this future. It seems to behave more consistently
          submitChild(taskController)(using a)
          if (!closestFinish.isRoot) then
            val finishVal = closestFinish.totalChildren.decrementAndGet()
            if (finishVal == 0) {
              closestFinish.await()
            }
          // Signal the scheduler that this function has finished. This will decrement the cnt by one and possibly terminate the scheduler
          Scheduler.finish(taskController)
          // Have child resubmit finish when done

        catch // If an error is encountered then notify the scheduler of this
          case NonFatal(e) =>
            // Call the throwError method, which increments the number of exceptions and finishes this task
            Scheduler.throwError(e, taskController)
          case e =>
            Scheduler.throwError(e, taskController)
    }
    val closestFinish = taskController.closestType(ControllerType.Finish)
    if (!closestFinish.isRoot) then closestFinish.totalChildren.incrementAndGet()
    Scheduler.startThread(task, taskController)
    Scheduler.submit(taskController) // <- try variants below
}
 */
