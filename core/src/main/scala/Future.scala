package mccct

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.{Lock, ReentrantLock, Condition}

import scala.util.{Try, Success, Failure}
import gears.async
import async.Future.Promise
// Use timer / timertask instead of gears `Scheduler`
import gears.async.{Cancellable, Scheduler}
import gears.async.default.given

import scala.concurrent.duration.{FiniteDuration, SECONDS, NANOSECONDS}

import scala.util.control.NonFatal
import java.io.{File, FileWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.CyclicBarrier

object Future {
  def apply[T](body: Controller ?=> T)(using a: async.Async, parent: Controller): Future[T] =
    val p          = Promise[T]()
    val controller = new Controller(parent)
    val task       = new Runnable:
      def run() =
        try
          controller.await() // Wait for scheduler to let the task start
          // Using a promise is enough, since a task is started only once
          // Try to execute the body
          val result = body(using controller)
          // Since the body has been run, we can add the end task
          Scheduler.addEndTask(controller)
          // Signal the scheduler that this function has finished. This will submit children and end, decrement the taskCount by one and possibly terminate the scheduler
          Scheduler.finish(controller)
          p.complete(Success(result))
        catch // If an error is encountered then notify the scheduler of this
          case e =>
            // Call the throwError method, which increments the number of exceptions, and finishes this task
            Scheduler.throwError(controller)
            p.complete(Failure(e)) // Complete the promise/future as a failure

    // Start task on virtual thread
    Scheduler.startThread(task, controller)
    // Add task to parent
    parent.addAssociatedTask(controller)
    new Future(p.asFuture)
}

class Future[T](underlying: async.Future[T]) {

  def value: Option[Try[T]] = underlying.poll()

  def isCompleted: Boolean = underlying.poll().nonEmpty

  def await(using ac: async.Async, controller: Controller): T =
    // We submit associated tasks since the underlying future might be a child
    Scheduler.submitMultiple(controller.getAndClearAssociatedTasks())
    // We are waiting for progress to be made and for the controller to be resubmitted, so the current task becomes inactive
    Scheduler.decrementActiveTasks()
    val resultOrFailure =
      underlying.awaitResult // Wait for the underlying future (the one that is awaited) to finish before continuing
    // Since this task has already been accounted for, we do not count this task when it is resubmitted.
    // ! Problematic, introduces non-determinism since the task completing the future can submit associated tasks before or after
    Scheduler.submit(controller, false)
    // Wait for scheduler to resume task
    controller.await()
    resultOrFailure.get
}

object Scheduler {

  private var done               = false
  private[mccct] var debug       = false
  private val numErrors          = AtomicInteger(0)
  private[mccct] var hasTimedOut = false
  // ! Previously `cnt`, now `taskCount`
  private var taskCount   = 0                // The number of currently running tasks, used for termination
  private val activeTasks = AtomicInteger(0) // The number of currently running tasks, used for sequential execution
  private[mccct] var isSequential = true
  private val lock: Lock          = new ReentrantLock
  private val queueChange         = lock
    .newCondition() // Used to control the scheduler. Is signaled when a new task is added to the readyTasks list, if a sequential task has become inactive, or if the scheduler should terminate
  private val termination =
    lock.newCondition() // Used by the main thread to wait until all tasks have finished executing

  val runningActors = AtomicInteger(0)

  private var readyTasks: Vector[Controller] =
    Vector() // Tasks that the exploration algorithm can choose to execute

  private var schedule: List[String] = List() // The recorded schedule

  private var startedThreads = List[(Thread, Controller)]()

  /** Determines if a task has been submitted to the scheduler, so that we do not finish immediately
    */
  private var hasSubmitted: Boolean = false

  private var addEndTasks: Boolean = true

  def apply[T](
      alg: ExplorationAlgorithm = RandomWalk,
      shouldPrint: Boolean = false,
      sequential: Boolean = false,
      includeTaskEndings: Boolean = true
  )(
      body: (Controller, gears.async.Async) ?=> T
  ): Unit =
    val rootController = new Controller(null)
    val rootTask       = new Runnable {
      def run() =
        try
          // Wait for scheduler to let the task start
          rootController.await()
          // Run the body of the root
          gears.async.Async.blocking {
            body(using rootController)
          }
          // The body has been run, so we can add the end task
          Scheduler.addEndTask(rootController)
          // Signal the scheduler that this function has finished.
          // This will submit children and end task, decrement the taskCount by one and possibly terminate the scheduler.
          Scheduler.finish(rootController)
        catch // The scheduler is notified if an error occurs
          case _ =>
            Scheduler.throwError(rootController)
    }
    Scheduler.start(alg, shouldPrint, sequential, includeTaskEndings, backwardsCompatible = false)
    // Start task on virtual thread
    Scheduler.startThread(rootTask, rootController)
    // Submit the root to CCT scheduler
    Scheduler.submit(rootController)
    // Wait for root and potentially created children to terminate
    Scheduler.awaitTermination(backwardsCompatible = false)(using rootController)

  /** Function for parent task to add its end (.0.) task.
    *
    * When the end task is completed the scheduler and user knows for sure that the parent is also complete (code wise).
    * This enables algorithms to force a task to complete before allowing other tasks to run.
    *
    * @param parent,
    *   the task that is submitting the child task (will have this as its last child task)
    */
  // ! This function replaces the different submitChild functions, since it is the same for all tasks
  def addEndTask(parent: Controller): Unit =
    if addEndTasks then
      val endController = new Controller(parent, isEnd = true)
      val emptyTask     = new Runnable { // The task that is executed on a new thread
        def run() = {
          try
            endController.await()           // Wait for scheduler to signal the controller to execute
            Scheduler.finish(endController) // Do nothing and finish
          catch
            case _ =>
              Scheduler.throwError(endController)
        }
      }
      Scheduler.startThread(emptyTask, endController) // Start this .0. child task on a new virtual thread
      parent.addAssociatedTask(endController)

  def start(
      alg: ExplorationAlgorithm = RandomWalk,
      shouldPrint: Boolean = false,
      sequential: Boolean = true,
      includeTaskEndings: Boolean = true,
      backwardsCompatible: Boolean = true
  ): Unit =
    Scheduler.reset() // In case the scheduler has been used before, reset it so no information is carried over
    lock.lock()
    try
      debug = shouldPrint
      isSequential = sequential
      addEndTasks = includeTaskEndings
      // TODO: Remove backwards compatible mode when it's not needed anymore
      if backwardsCompatible then
        // Treat the code between `start` and `awaitTermination` as a submitted root task
        // Will not include 0. (nor 0.0.) task in schedule, and is incapable of handling errors.
        // Otherwise, the functionality should be equivalent.
        taskCount += 1
        activeTasks.getAndIncrement()
        hasSubmitted = true
        if debug then println(s"Old root ([0.]) is used (taskCount=$taskCount, activeTasks=${activeTasks.get()})")
    finally lock.unlock()
    val schedulerTask = new Runnable {
      def run() =
        while (true) {
          lock.lock()
          try
            // If the scheduler is run sequentially we wait until no other tasks are running
            if sequential then
              waitSequential()
              if debug then println(s"Queue after sequential wait is: $readyTasks")
            // If the scheduler needs more data then wait
            // Repeatedly called until list is non empty
            // For parallel it should act as an IF
            // For sequential execution there can be multiple queueSignals that do not update readyTasks
            while readyTasks.isEmpty && !hasFinished() do
              if debug then
                println(
                  s"Scheduler waiting for tasks... (taskCount=${taskCount}, activeTasks=${activeTasks.get()})"
                )
              waitTasks() // Wait until we either have a task to execute, or if the scheduler has finished

            // If the scheduler reaches this one of two possibilities must be true
            // Either readyTasks is empty, which means that the queueChange signal was triggered because the scheduler should terminate
            // Or readyTasks is non-empty and the scheduler should continue execution
            if hasFinished() then
              if debug then println("Scheduler has finished.")
              done = true
              termination.signal()
              return
            if debug then println(s"Scheduler has non-empty queue (len=${readyTasks.size}): ${readyTasks}")
            val nextTasks = getNextTasks(alg) // Get the next task as specified by the algorithm and its controller
            // Execution tasks can be `None` if timeout happened while the scheduler was waiting for the correct task
            nextTasks match
              case Some(value) =>
                readyTasks =
                  if readyTasks.eq(value) then Vector()
                  else
                    readyTasks diff value // Remove the task (and its controller) from the readyTasks list, since the same task should not be allowed to be started more than once
                if debug then println(s"\tNumber of tasks selected to execute: ${nextTasks.size}")
                executeTask(value)
              case None =>
                if debug then println("Scheduler terminating from timeout...")
                done = true
                // ! I do not think we are guaranteed to have reached `awaitTermination` in this case, if there is an await or checkSuspend on in the root level for example
                termination.signal()
                return
          finally lock.unlock()
        }
    }
    // Start the scheduler on a new thread
    Thread.ofPlatform().start(schedulerTask)

  private def waitSequential(): Unit =
    // If the scheduler has at least one running task, then the scheduler must wait until it finishes before continuing
    // If readyTasks is empty, the scheduler must wait until we get a new task in it
    // If the scheduler has finished do not wait
    while activeTasks.get() > 0 && !hasFinished() do awaitQueueChange()
    // Can get a signal and not updated list, in these cases the scheduler has `waitTasks`

  /** Replaces manuals calls to `queueChange.signal()`
    *
    * @param all
    *   uses `signalAll` instead of `signal`
    */
  // ! Previously unused function `triggerQueueChange`
  private[mccct] def signalQueueChange(all: Boolean = false): Unit =
    lock.lockInterruptibly()
    try
      if all then queueChange.signalAll()
      else queueChange.signal()
    finally lock.unlock()

  /** Replaces manuals calls to `queueChange.await()`
    *
    * @param uninterruptibly
    *   uses `awaitUninterruptibly` instead of `await`
    */
  private[mccct] def awaitQueueChange(uninterruptibly: Boolean = false): Unit =
    lock.lockInterruptibly()
    try
      if uninterruptibly then queueChange.awaitUninterruptibly()
      else queueChange.await()
    finally lock.unlock()

  /** Used outside of the scheduler to signal that a task is no longer active, allowing for other tasks to be started
    * (relevant for sequential mode)
    */
  // ! Previously `decrementSequential`
  private[mccct] def decrementActiveTasks(): Unit =
    lock.lockInterruptibly()
    try
      activeTasks.decrementAndGet()
      if isSequential then queueChange.signal() // Signal that a change has been made to the Scheduler
    finally lock.unlock()

  // If the scheduler has tasks in the queue it does not need to wait
  // Since there is no guarantee that there will be other tasks added to the queue if it is non-empty
  // Therefore, the scheduler should make a choice
  // Furthermore, it is possible that the queueChange signal for termination has been sent at the end of the while loop
  // In this case the scheduler will get no more queueChange signals, therefore the scheduler must be able to skip the await (or it gets stuck)
  private def waitTasks(): Unit =
    if readyTasks.isEmpty && !hasFinished() then awaitQueueChange()

  /** Tail-recursive function that returns the next task to execute
    *
    * @param alg
    *   The algorithm which determines what task to execute and how to choose it
    * @return
    *   the task to be executed
    */
  private def getNextTasks(alg: ExplorationAlgorithm): Option[Vector[Controller]] =
    alg.getNext(readyTasks) match
      case Some(l) =>
        Some(l)
      // If algorithm returns None it indicates that the algorithm is not satisfied with the readyTasks list.
      case None => { // There is non-empty queue, however it has the wrong elements
        awaitQueueChange() // Therefore, the scheduler should wait for an update until the algorithm returns a non-empty option
        if hasTimedOut then return None
        if hasFinished() then // Should not be possible
          assert(false)       // Since readyTasks should always be non-empty if this line is reached
        getNextTasks(alg)
      }

  def awaitTermination(backwardsCompatible: Boolean = true)(using rootController: Controller) =
    lock.lock()
    try
      // TODO: Remove when backwards compatibility is no longer needed
      if backwardsCompatible then Scheduler.finish(rootController)
      // Wait for scheduler to complete
      termination
        .awaitUninterruptibly() // Since the main thread has the lock, the termination signal can not be sent before the await
      if debug then println("Scheduler has terminated.")
      // If a timeout has happened an error should be thrown
      if hasTimedOut then throw new DeadlockException
    finally
      schedule =
        schedule.reverse // Since the tasks are prepended to the schedule history, the list must be reversed to get history in the correct order
      lock.unlock()

  private def hasFinished(): Boolean =
    taskCount <= 0
      && readyTasks.size == 0
      && runningActors.get() == 0
      && hasSubmitted

  private[mccct] def submit(
      controller: Controller,
      shouldIncrement: Boolean = true
  ): Unit =
    lock.lockInterruptibly()
    try
      if !hasTimedOut then
        // Should the task be counted as a new task or not (for example if it has already been started but had to wait)
        if shouldIncrement then taskCount += 1
        // New task is appended to the end of the queue
        readyTasks = readyTasks :+ controller
        hasSubmitted = true // Used so the scheduler does not finish prematurely
        if debug then
          println(s"Task $controller was submitted, incremented taskCount: $shouldIncrement (taskCount=$taskCount)")
        // Allow scheduler to continue if waiting for tasks
        signalQueueChange()
    finally lock.unlock()

  private[mccct] def submitMultiple(tasks: List[(Controller, Boolean)]): Unit =
    if tasks.nonEmpty then // Only does something if there actually are tasks to submit
      lock.lockInterruptibly()
      try
        if !hasTimedOut then
          taskCount += tasks.count(p => p._2)
          // New tasks are appended to the end of the queue, reversed since the task list is prepended to
          readyTasks = readyTasks :++ tasks.reverse.map(p => p._1)
          hasSubmitted = true // Used so scheduler do not finish prematurely
          if debug then
            println(
              s"Tasks ${tasks.reverse.map(p => p._1)} were submitted, increment count: ${tasks.count(p => p._2)} (taskCount=$taskCount)"
            )
          // Allow scheduler to continue if waiting for tasks
          signalQueueChange()
      finally lock.unlock()

  private[mccct] def getSchedule(): List[String] = schedule

  private[mccct] def finish(controller: Controller, shouldDecrement: Boolean = true): Unit =
    lock.lock()
    try
      // We submit all the tasks that were associated with the controller (e.g. children)
      Scheduler.submitMultiple(controller.getAndClearAssociatedTasks())
      // The current task is finished
      Scheduler.decrementActiveTasks()
      if shouldDecrement then taskCount -= 1
      // If this was the last task to complete we should signal the scheduler so it can terminate
      if hasFinished() then signalQueueChange()
      if debug then
        println(
          s"Task $controller was finished, decremented taskCount: $shouldDecrement (taskCount=$taskCount, activeTasks=${activeTasks.get()})"
        )
    finally lock.unlock()

  private def executeTask(executionTasks: Vector[Controller]): Unit =
    executionTasks.foreach { controller =>
      // We are starting/continuing a task
      activeTasks.getAndIncrement()
      // Add the id of the task to the history/schedule of executed tasks (this run of the schedule)
      schedule = controller.id.getId() :: schedule
      if debug then
        println(
          s"\t\tScheduler signalling task $controller to continue (taskCount=${taskCount}, activeTasks=${activeTasks.get()})\n"
        )
      lock.unlock()
      // Signal the task to start
      controller.await()
      lock.lock()
    }

  /** A function that starts a task on a virtual thread
    *
    * Is used to for the scheduler to know which threads have been created, and to be able to interrupt them
    *
    * @param task
    */
  private[mccct] def startThread(task: Runnable, controller: Controller): Unit =
    lock.lock()
    try
      if !hasTimedOut then
        val v = controller.startThread(task, startedThreads.size)
        startedThreads = (v, controller) :: startedThreads
    finally
      lock.unlock()

  def threads: List[(Thread, Controller)] = startedThreads

  /** A function that is called to timeout the scheduler, terminating threads and writing the schedule.
    *
    * The function will (if the scheduler has not already timed out) print a message identifying which `schedulePoint`
    * resulted in the timeout and what controller it was a part of.
    *
    * After this all non-root tasks, will be removed from the readyTasks list, and the schedule up until this point will
    * be written to file. Lastly, all running task threads will be interrupted.
    *
    * @param id,
    *   the id of the `schedulePoint` that resulted in the timeout
    * @param ctrl,
    *   the controller in which `schedulePoint` was called
    */
  private def timeoutThreads(id: Int, write: Boolean, controller: Controller): Unit =
    lock.lockInterruptibly()
    try
      readyTasks = Vector()
      println(
        s"A possible deadlock has occurred for controller ${controller}\nThe `schedulePoint` that triggered this timeout had id: ${id}"
      )
      if write then writeSchedule()
      startedThreads.map((t, c) =>
        if !t.isInterrupted() then
          c.addTimeoutTask(None) // Remove the scheduled timeout task if any
          t.interrupt()          // Then interrupt the thread
      )
      signalQueueChange()
    finally lock.unlock()

  /** A function that creates a scheduled timeout task, suspending execution of running tasks.
    *
    * @param id,
    *   the id of the `schedulePoint` that resulted in the timeout
    * @param delay,
    *   the delay which the scheduler should wait before timeout
    * @param task,
    *   the task that called the function
    */
  private def addTimeout(id: Int, write: Boolean, delay: FiniteDuration)(using
      scheduler: async.Scheduler,
      controller: Controller
  ): Cancellable =
    scheduler.schedule(
      delay,
      new Runnable {
        def run() =
          lock.lockInterruptibly()
          try
            if !hasFinished() then
              // If the controller is in readyTasks then it is not necessarily stuck, it may just be waiting for execution (for example in sequential mode).
              // In this case a timeout should not be thrown, instead restart the timeout.
              // Otherwise, call `timeoutThreads`
              if !readyTasks.contains(controller) then
                hasTimedOut = true
                timeoutThreads(id, write, controller)
              else controller.addTimeoutTask(Some(addTimeout(id, write, delay)))
          finally
            lock.unlock()
      }
    )

  /** Adds a scheduling point in the program that can allow other tasks to run.
    */
  def schedulePoint(
      id: Int = 0,
      timeout: Boolean = false,
      write: Boolean = true,
      delay: FiniteDuration = FiniteDuration(3, SECONDS)
  )(using
      controller: Controller
  ): Unit =
    lock.lockInterruptibly()
    try
      // If it should be able to timeout, then add a new timeout task
      if timeout && !hasTimedOut then controller.addTimeoutTask(Some(addTimeout(id, write, delay)))
      // Otherwise, add nothing
      else controller.addTimeoutTask(None)
    finally
      lock.unlock()
    // We resubmit the controller so it can be selected by the scheduler.
    // Since this task has already been accounted for, we do not count this task when it is resubmitted.
    controller.addAssociatedTask(controller, false)
    // Submit associated tasks, along with the current task
    Scheduler.submitMultiple(controller.getAndClearAssociatedTasks())
    // The current task is no longer running
    Scheduler.decrementActiveTasks()
    // Wait until the task can resume
    controller.await()

  def reset(): Unit =
    lock.lock()
    try
      done = false
      readyTasks = Vector()
      taskCount = 0
      runningActors.set(0)
      // ! Not needed in new usage method, since the root controller is now explicitly created by the scheduler
      Controller.rootController.id.reset()
      schedule = List()
      numErrors.set(0)
      activeTasks.set(0)
      startedThreads = List()
      hasTimedOut = false
      hasSubmitted = false
      addEndTasks = true
    finally lock.unlock()

  def getDone(): Boolean = done

  def getNumErrors(): Int = numErrors.get()

  /** A function that takes a function that uses futures and an expected result and returns if the tested function
    * results in NO errors, and if errors reliably occur with the given schedule, and writes the used schedule to file
    * if the schedule reliably gives errors.
    *
    * The function tests this by calling the function `checkReliability` and `checkErrors`.
    *
    * @param testedFunc,
    *   the function to be tested
    * @param expectedOutput,
    *   the expected output of the function
    * @param outputFile,
    *   the file name in which to write the schedule (if possible)
    * @return
    */
  def handleErrors[T](
      testedFunc: => T,
      expectedOutput: T,
      schedule: List[String],
      outputFile: String = "trace.txt",
      sequential: Boolean = false,
      iters: Int = 10,
      acceptRate: Double = 0.75,
      debug: Boolean = false
  ): (Boolean, Boolean) = {
    val reliable = checkReliability(
      testedFunc,
      expectedOutput,
      schedule,
      sequential = sequential,
      iters = iters,
      acceptRate = acceptRate,
      debug = debug
    )
    val hasNoErrors = checkErrors(true)
    if reliable then writeSchedule(outputFile)
    (hasNoErrors, reliable)
  }

  /** A function that returns true if the scheduler has encountered NO errors in its last run and if the given assertion
    * holds.
    *
    * Otherwise, if some errors have occured, then the function returns false.
    *
    * @param assertion,
    *   the given assertion to test (for example if output is as expected).
    * @return
    */
  private[mccct] def checkErrors(assertion: Boolean): Boolean = {
    try
      assert(assertion) // Check if input assertion holds

      assert(numErrors.get() == 0) // Check that no errors were encountered
      true                         // True representing that no errors were encountered
    catch                          // If any of the assertions fails
      case NonFatal(e) =>
        if debug then
          println("Exception was encountered")
          println(s"Number of errors: ${numErrors.get()}")
        false // False representing that some error was encountered
  }

  /** A function that repeatedly tests a given function on a given schedule to see if it (most of the time) gives the
    * same expected result.
    *
    * Returns `true` if the given schedule reliably gives errors (more than `acceptRate` percentage of times), otherwise
    * false.
    *
    * @param func,
    *   the function to be tested
    * @param expected,
    *   the expected result of `func`
    * @param erroneousSchedule,
    *   the schedule to test `func` on
    * @param iters,
    *   how many times `func` should be tested
    * @param acceptRate,
    *   the minimum percentage of runs that have to either give error or not get expected output for the function to
    *   return true
    * @return
    */
  private[mccct] def checkReliability[T](
      func: => T,
      expected: T,
      erroneousSchedule: List[String],
      iters: Int = 10,
      acceptRate: Double = 0.75,
      sequential: Boolean = false,
      debug: Boolean = false
  ): Boolean = {
    var i           = 0
    var numExpected = 0
    while (i < iters) {
      println(s"Checking reliability iteration ${i + 1}/$iters")
      Scheduler.start(FixedSchedule(erroneousSchedule), shouldPrint = debug, sequential = sequential)
      val res = func
      Scheduler.awaitTermination()
      numExpected += (if checkErrors(res != expected) then 0 else 1)
      i += 1
    }
    val rate: Double = numExpected.toDouble / iters.toDouble
    println(s"Rate was: $rate")
    rate >= acceptRate
  }

  private[mccct] def throwError(controller: Controller): Unit = {
    numErrors.incrementAndGet()  // If an error is thrown, increment the number of errors we have encountered
    Scheduler.finish(controller) // Then signal the scheduler that this task has finished (allowing for termination)
  }

  def readSchedule(fileName: String): List[String] = {
    var fileData       = ""                                 // The read data
    val bufferedSource = scala.io.Source.fromFile(fileName) // Get the data as a buffered source
    for (lines <- bufferedSource.getLines()) {
      fileData = fileData + lines // Append each line to the fileData
    }
    bufferedSource.close()     // Close the file
    fileData.split(",").toList // Split the data into the correct strings
  }

  def scheduleToString(): String = schedule.mkString(", ")

  def writeSchedule(fileName: String = "", id: String = "", potentialTarget: Option[List[String]] = None): Unit = {
    if debug then println("Writing schedule to file")
    val target =
      if potentialTarget.isDefined
      then potentialTarget.get
      else if !done then
        // If the scheduler has not finished by the time this function is called, then it means that the schedule is in the reverse order
        schedule.reverse
      else schedule

    if !done then
      // If the scheduler has not finished by the time this function is called, then it means that the schedule is in the reverse order
      schedule = schedule.reverse
    var file = fileName

    // If no fileName was specified then generate a file name from the current date and time
    if file == "" then
      val currentDateTime = LocalDateTime.now()
      val formatter       = DateTimeFormatter.ofPattern("dd-MM-yy-HH-mm-ss")
      val formattedDate   = currentDateTime.format(formatter)
      file = "trace_" + id + "-" + formattedDate + ".txt"

    val fileWriter = new FileWriter(new File(file)) // Open and create a new file with the given name
    // An option to this is to write each task on a new line, this would make parsing the file into a oneliner, however long files can be a bit hard to work with.
    fileWriter.write(target.mkString(",")) // Write the task ids seperated by ","
    fileWriter.write(", ")                 // Have space at the end to make it possible to read ",,"
    fileWriter.close()                     // Close the file writer

    // Switch back the history schedule as it was before
    if !done then schedule = schedule.reverse
  }
}
