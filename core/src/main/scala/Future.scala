package mccct

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.{Lock, ReentrantLock, Condition}

import scala.util.{Try, Success, Failure}
import gears.async
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
import scala.collection.mutable.TreeMap
import scala.quoted.{Expr, Quotes}

object Future {

  /** Function for parent task to add its .0. child task
    *
    * When the .0. task is completed the scheduler and user knows for sure that the parent is also complete (code wise)
    * This enables algorithms to force a future to complete before allowing other futures to run
    *
    * The `submitChild` function is almost a replica of the apply-function with the main difference being that the .0.
    * child does not do anything The added child will only await its execution, and when it is allowed to continue it
    * will finish()
    *
    * @param parent,
    *   the task that is submitting the child task (will have this as its last child task)
    * @param a
    *   async context
    */
  private def submitChild(parent: Controller)(using a: async.Async): Unit = {
    val taskController = new Controller(parent, isEnd = true)
    val task           = new Runnable { // The task that is executed on a new thread
      def run() = {
        try
          taskController.await()           // Wait for scheduler to signal the controller to execute
          Scheduler.finish(taskController) // Do nothing and finish()
        catch
          case e =>
            Scheduler.throwError(e, taskController)
      }
    }
    Scheduler.startThread(task, taskController) // Start this .0. child task on a new virtual thread
    Scheduler.submit(taskController)            // Submit the child task to the scheduler
  }

  def apply[T](body: Controller ?=> T)(using a: async.Async, parent: Controller): Future[T] =
    val p              = async.Future.Promise[T]()
    val taskController = new Controller(parent)
    val task           = new Runnable {
      def run() =
        try
          taskController.await() // Wait for scheduler to let the task start
          // Using a promise is enough, since a task is started only once
          // Try to execute the body
          val result = body(using taskController)
          // Schedule the end child before completing this future. It seems to behave more consistently
          submitChild(taskController)
          // Signal the scheduler that this function has finished. This will decrement the cnt by one and possibly terminate the scheduler
          Scheduler.finish(taskController)
          p.complete(Success(result))
        catch // If an error is encountered then notify the scheduler of this
          case NonFatal(e) =>
            // Call the throwError method, which increments the number of exceptions and finishes this task
            Scheduler.throwError(e, taskController)
            p.complete(Failure(e)) // Complete the promise/future as a failure
          case e =>
            Scheduler.throwError(e, taskController)
            p.complete(Failure(e)) // Complete the promise/future as a failure
    }
    // Start task on virtual thread
    Scheduler.startThread(task, taskController)
    // Submit new ready task to CCT scheduler
    Scheduler.submit(taskController)
    new Future(p.asFuture)
}

class Future[T](underlying: async.Future[T]) {

  def value: Option[Try[T]] = underlying.poll()

  def isCompleted: Boolean = underlying.poll().nonEmpty

  def await(using ac: async.Async, controller: Controller): T = {

    /** Signal the scheduler that we are waiting for something If the task calling await is top-level, then `task` will
      * be the root task Waiting for a top-level task means that the scheduler is in a "stuck" state and must execute
      * the awaited task before it is able to continue
      */
    Scheduler.stuckSignal(controller)
    // When the task awaits and the scheduler is running sequentially, then the scheduler can allow another task to run
    Scheduler.decrementSequential(controller)
    val resultOrFailure =
      underlying.awaitResult // Wait for the underlying future (the one that is awaited) to finish before continueing
    // inform CCT scheduler --> should move task to ready queue
    Scheduler.submit(
      controller,
      false
    ) // Since the cnt of this task has already been accounted for do not increase the cnt again when this task is resubmitted to the scheduler

    // wait for scheduler to resume task
    controller.await()

    /** Signal the scheduler that it is no longer in a stuck state If a top level task was awaited then the Scheduler
      * should now suspend execution until all top-level tasks have been submitted to the scheduler or the scheduler
      * reaches another stuck state
      *
      * Also allows the scheduler to terminate
      */
    Scheduler.noLongerStuck(controller)

    resultOrFailure match // Now we have to match the result correctly
      // If it was a failure on the main thread, that would result in throwing an error on the main thread then
      case Failure(e) if controller.isRoot =>
        // Terminate the scheduler prematurely (since the "normal" `awaitTermination` can not be reached)
        Scheduler.awaitTermination()
      case _ => ()

    resultOrFailure.get
  }
}

object Scheduler {

  private var done               = false
  private[mccct] var debug       = false
  private val numErrors          = AtomicInteger(0)
  private[mccct] var hasTimedOut = false
  private var cnt                = 0 // The number of currently running tasks, used for termination
  private var activeTasks = AtomicInteger(0) // The number of sequentially running tasks, used for sequential execution
  private[mccct] var isSequential = false
  private val lock: Lock          = new ReentrantLock
  private val queueChange         = lock
    .newCondition() // Used to control the scheduler. Is either signaled when a new task is added to the readyTasks list or if the scheduler should terminate
  private val termination =
    lock.newCondition() // Used by the main thread to wait until all tasks have finished executing
  /** Used to signal when execution from a stuck state must continue */
  private val stuckState = lock.newCondition()

  val runningActors = AtomicInteger(0)

  private var recordFailureInjections                       = true
  private var failureMapping: TreeMap[Int, Vector[Boolean]] = TreeMap[Int, Vector[Boolean]]()
  private var failureAlgorithm: FailureExplorationAlgorithm = NeverInject

  private var readyTasks: List[Controller] =
    List() // The list of readyTasks in which the exploration algorithm can choose one to execute

  private var schedule: List[String] = List() // The recorded schedule

  /** Determines if all top-level tasks have been loaded. If true, then the scheduler knows that it may terminate and
    * that all future tasks has to be the children of current tasks
    */
  private var hasAllTasks: Boolean = false

  private var startedThreads = List[(Thread, Controller)]()

  def start(
      alg: ExplorationAlgorithm = RandomWalk,
      shouldPrint: Boolean = false,
      sequential: Boolean = false,
      failureAlg: FailureExplorationAlgorithm = NeverInject,
      recordFailures: Boolean = true
  ): Unit =
    Scheduler.reset() // In case the scheduler has been used before, reset it so no information is carried over
    lock.lock()
    try
      debug = shouldPrint
      isSequential = sequential
      failureAlgorithm = failureAlg
      recordFailureInjections = recordFailures
    finally lock.unlock()
    val schedulerTask = new Runnable {
      def run() =
        while (true) {
          lock.lock()
          try
            // If hasAllTasks is false, then the main thread can still load and submit more top-level tasks
            // In this case the scheduler will wait until it must execute, to give all top-level tasks an equal chance to be executed
            // If hasAllTasks is true, then no more top-level tasks will be started. This means that new tasks will only be a product/child of current tasks.
            // Therefore, we can continue execution until we terminate
            if !hasAllTasks then stuckState.awaitUninterruptibly()
            // If the scheduler is run sequentially follow those rules
            if sequential then waitSequential()
            if debug then println("List after seq wait is: " + readyTasks.map(t => t))
            // If the scheduler needs more data then wait
            // Repeatedly be called until list is non empty
            // For parallel it should act as an IF
            // For sequential execution there can be multiple queueSignals that do not update readyTasks
            while (!hasFinished && readyTasks.size == 0) {
              if debug then println(s"Waiting for task (${cnt}, ${readyTasks})")
              waitTasks() // Wait until we either have a task to execute, or if the scheduler has finished
            }
            if debug then println(s"Got non-empty queue (${readyTasks.map(t => t)})")
            // If the scheduler reaches this one of two possbilities must be true
            // Either readyTasks is empty, which means that the queueChange signal was triggered because the scheduler should terminate
            // Or readyTasks is non-empty and the scheduler should continue execution
            if debug then println("\t\tHas finished is: " + hasFinished)
            if hasFinished then
              done = true
              termination.signal()
              return
            if debug then println(s"scheduler: size of task queue = ${readyTasks.size}")
            val executionTasks = getNextTask(alg) // Get the next task as specified by the algorithm and its controller
            // Execution tasks can be `None` if timeout happened while the scheduler was waiting for the correct task
            executionTasks match
              case Some(value) =>
                readyTasks =
                  if readyTasks.eq(value) then List()
                  else
                    readyTasks diff value // Remove the task (and its controller) from the readyTasks list, since the same task should not be allowed to be started more than once
                if debug then println("\t\tnumber of tasks to execute: " + executionTasks.size + "\n")
                executeTask(value)
              case None =>
                if debug then println("Terminating from timeout...")
                done = true
                termination.signal()
                return
          finally lock.unlock()
        }
    }
    // Start the scheduler on a new thread
    Thread.ofPlatform().start(schedulerTask)

  private def waitSequential(): Unit =
    // If the scheduler has atleast one running task, then the scheduler must wait until it finishes before continuing
    // If the readytasks is empty, the scheduler must wait until we get a new task in it
    // If the scheduler has finished do not wait
    while (activeTasks.get() > 0 && !hasFinished) {
      queueChange.awaitUninterruptibly()
    }
    // Can get a signal and not updated list, in these cases the scheduler has `waitTasks`

  private[mccct] def triggerQueueChange(): Unit =
    lock.lockInterruptibly()
    try
      queueChange.signal()
    finally
      lock.unlock()

  /** A function that is called when a task is awaited. This makes sure that a new task can be started when a task is
    * awaited
    */
  private[mccct] def decrementSequential(ctrl: Controller): Unit =
    lock.lockInterruptibly()
    try
      // If the scheduler is in sequential mode
      if isSequential && !ctrl.isRoot then
        activeTasks.getAndDecrement() // Then decrement the number of active tasks
        queueChange.signal()          // And signal that a change has been made to the Scheduler
    finally
      lock.unlock()

  // If the scheduler has tasks in the queue it does not need to wait
  // Since there is no guarantee that there will be other tasks added to the queue if it is non-empty
  // Therefore, the scheduler should make a choice
  // Furthermore, it is possible that the queueChange signal for termination has been sent at the end of the while loop
  // In this case the scheduler will get no more queueChange signals, therefore the scheduler must be able to skip the await (or it gets stuck)
  private def waitTasks(): Unit =
    if (readyTasks.size == 0 && !hasFinished) then queueChange.awaitUninterruptibly()

  /** Tail-recursive function that returns the next task to execute
    *
    * @param alg
    *   The algorithm which determines what task to execute and how to choose it
    * @return
    *   the task to be executed
    */
  private def getNextTask(alg: ExplorationAlgorithm): Option[List[Controller]] =
    alg.getNext(readyTasks) match
      case Some(l) =>
        Some(l)
      // If algorithm returns None it indicates that the algorithm is not satisfied with the readyTasks list.
      case None => { // There is non-empty queue, however it has the wrong elements
        queueChange
          .awaitUninterruptibly() // Therefore, the scheduler should wait for an update until the algorithm returns a non-empty option
        if hasTimedOut then return None
        if hasFinished then // Should not be possible
          assert(false)     // Since readyTasks should always be non-empty if this line is reached
        getNextTask(alg)
      }

  def awaitTermination(requireAction: Boolean = false) =
    lock.lock()
    if done then lock.unlock() // Avoid deadlock if multiple `awaitTermination` are used
    else
      try
        // The end of the main thread has been reached
        hasAllTasks = true // All top level tasks must now be available for the scheduler
        stuckState.signalAll()
        if hasFinished
        then // If we have finished before calling awaitTermination Scheduler will be waiting for queueChange
          queueChange.signalAll() // Signal the Scheduler a queueChange to get termination signal
        termination
          .awaitUninterruptibly() // Since the main thread has the lock, the termination signal can not be sent before the await
        // If a timeout has happened an error should be thrown
        if hasTimedOut then throw new DeadlockException
      finally
        schedule =
          schedule.reverse // Since the tasks are prepended to the schedule history, the list must be reversed to get history in the correct order
        lock.unlock()

  /** Signals the scheduler to execute if in stuck state
    *
    * If task is a root task, then the awaited task must be a top level task In this case signal the scheduler to
    * execute until other instructions are given
    * @param task
    *   the task that has been suspended
    */
  private[mccct] def stuckSignal(ctrl: Controller) =
    lock.lockInterruptibly()
    try
      // If parent is null, then this must be a root task
      if ctrl.isRoot then   // Which means that execution must continue until the awaited top-level task is completed
        hasAllTasks = true  // Allow scheduler to execute until `hasAllTasks` is set to false
        cnt += 1            // Make sure that the scheduler can not finish while waiting for a top-level task
        stuckState.signal() // Signal the lock that the scheduler must continue
    finally
      lock.unlock()

  /** Signals the scheduler to wait until stuck or all top-level tasks have been loaded
    *
    * If task is a root task, then the awaited task must be a top level task In this case signal the scheduler to wait
    * until notified otherwise
    * @param task
    *   the task that has been suspended
    */
  private[mccct] def noLongerStuck(ctrl: Controller) =
    lock.lockInterruptibly()
    try
      // If parent is null, then this must be a root task
      if ctrl.isRoot then // Which means that execution must continue until the awaited top-level task is completed
        hasAllTasks =
          false // In this case we have now executed the blocking task, and may once again wait until we have loaded all top-level tasks (or become stuck)
        cnt -= 1 // Scheduler may now finish when possible
    finally
      lock.unlock()

  private def hasFinished: Boolean =
    cnt <= 0 && readyTasks.size == 0 && hasAllTasks && runningActors.get() == 0

  private[mccct] def submit(
      ctrl: Controller,
      shouldIncrement: Boolean = true
  ): Unit =
    lock.lockInterruptibly()
    try
      if !hasTimedOut || ctrl.isRoot then
        if shouldIncrement
        then // Should the task be counted as a new task or not (for example if it has already been started but had to wait)
          cnt += 1
        readyTasks = ctrl :: readyTasks
        queueChange.signal()
    finally lock.unlock()

  def getSchedule(includeFailures: Boolean = true): List[String] =
    if !includeFailures then schedule
    else
      schedule.zipWithIndex.map { case (ctrl, idx) =>
        failureMapping.get(idx) match {
          case Some(failures) => ctrl + '|' + failures.map(b => if b then '1' else '0').mkString(".")
          case None           => ctrl
        }
      }

  private[mccct] def finish(ctrl: Controller, shouldDecrement: Boolean = true): Unit =
    lock.lock()
    try
      if shouldDecrement then cnt -= 1

      // If failure injection points were encountered during execution, we add what happened
      // at those points so that it is possible to append that information to the schedule
      failureMapping ++= ctrl.getFailures()

      activeTasks.getAndDecrement()
      if hasFinished then    // If this was the last task to complete and all tasks have been loaded then
        queueChange.signal() // If the Scheduler is in a state which should terminate, signal the queueChange
      else if isSequential then
        queueChange
          .signal() // If we are in sequential execution then signal a queueChange, should not result in termination
    finally lock.unlock()

  private def executeTask(executionTasks: List[Controller]): Unit =
    executionTasks.foreach { ctrl =>
      if debug then
        println(s"scheduler signalled (cnt=$cnt) with task: ${ctrl.toString()}")
        println(s"scheduler signalling task $ctrl to continue")
      if !ctrl.isRoot then activeTasks.getAndIncrement()
      // We let the controller start, and give it an index based on the current schedule
      ctrl.await(schedule.length)
      ctrl.reset()
      schedule = ctrl.id
        .getId() :: schedule // Add the id of the task to the history/schedule of executed tasks (this run of the schedule)
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
    * The function will (if the scheduler has not already timed out) print a message identifying which `checkSuspend`
    * resulted in the timeout and what controller it was a part of.
    *
    * After this all non-root tasks, will be removed from the readyTasks list, and the schedule up until this point will
    * be written to file. Lastly, all running task threads will be interrupted.
    *
    * @param id,
    *   the id of the `checkSuspend` that resulted in the timeout
    * @param ctrl,
    *   the controller in which `checkSuspend` was called
    */
  private def timeoutThreads(id: Int, write: Boolean, ctrl: Controller): Unit =
    lock.lockInterruptibly()
    try
      readyTasks = List()
      println(
        s"A possible deadlock has occured for ctrl ${ctrl}\nThe `checkSuspend` that triggered this timeout had id: ${id}"
      )
      if write then writeSchedule()
      startedThreads.map((t, c) =>
        if !t.isInterrupted() then
          c.addTimeoutTask(None) // Remove the scheduled timeout task if any
          t.interrupt()          // Then interrupt the thread
      )
      queueChange.signal()
    finally lock.unlock()

  /** A function that creates a sheduled timeout task, suspending execution of running tasks.
    *
    * @param id,
    *   the id of the `checkSuspend` that resulted in the timeout
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
            if !hasFinished then
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

  def checkSuspend(
      id: Int = 0,
      timeout: Boolean = false,
      write: Boolean = true,
      delay: FiniteDuration = FiniteDuration(3, SECONDS)
  )(using
      controller: Controller
  ): Unit =
    // Create a new taskController for the task
    // Allow another task to be started
    if !controller.isRoot then activeTasks.getAndDecrement()
    lock.lockInterruptibly()
    try
      // If it should be able to timeout, then add a new timeout task
      if timeout && !hasTimedOut then controller.addTimeoutTask(Some(addTimeout(id, write, delay)))
      // Otherwise, add nothing
      else controller.addTimeoutTask(None)
    finally
      lock.unlock()
    // Submit the task, thereby signaling the queueCHange
    submit(
      controller,
      false
    )
    controller.await() // Wait until the task can resume

  /** A method that can be instrumented in the code to inject failures based on the scheduler's failure algorithm, or on
    * a recorded schedule (takes precedence).
    *
    * @param failure
    *   the failure to inject
    * @param controller
    *   the controller in which `possibleFailure` was called
    */
  inline def possibleFailure(failure: Throwable)(using controller: Controller): Unit =
    // Defined as a macro to automatically instrument unique ids for each call site.
    ${ possibleFailureImpl('failure, 'controller) }

  private object InjectionPointCounter:
    private var counter: Int = 0
    def nextId(): Int        =
      val id = counter
      counter += 1
      id

  private def possibleFailureImpl(
      failure: Expr[Throwable],
      controller: Expr[Controller]
  )(using Quotes): Expr[Unit] =
    val id = InjectionPointCounter.nextId()
    '{ possibleFailureWithId(${ Expr(id) }, $failure)(using $controller) }

  private def possibleFailureWithId(id: Int, failure: Throwable)(using controller: Controller): Unit =
    if debug then println(s"Failure injection point (id=$id) invoked")
    val shouldInject =
      controller.hasScheduledChoice() match {
        case Some(choice) => choice
        case None         =>
          val choice = failureAlgorithm.shouldInject(id)
          controller.appendInjectionChoice(choice)
          choice
      }
    if debug then println(s"Will inject: $shouldInject")
    if shouldInject then throw failure

  def reset(): Unit =
    lock.lock()
    try
      done = false
      readyTasks = List()
      cnt = 0
      runningActors.set(0)
      Controller.rootController.id.reset()
      schedule = List()
      hasAllTasks = false
      debug = false
      numErrors.getAndSet(0)
      activeTasks.set(0)
      startedThreads = List()
      hasTimedOut = false
      recordFailureInjections = true
      failureMapping = TreeMap[Int, Vector[Boolean]]()
      failureAlgorithm = NeverInject
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

  private[mccct] def throwError(e: Throwable, ctrl: Controller): Unit = {
    numErrors.incrementAndGet() // If an error is thrown, increment the number of errors we have encountered
    finish(ctrl)                // Then signal the scheduler that this task has finished (allowing for termination)
  }

  def readSchedule(fileName: String): List[String] = {
    var fileData       = ""                                 // The read data
    val bufferedSource = scala.io.Source.fromFile(fileName) // Get the data as a buffered source
    for (lines <- bufferedSource.getLines()) {
      fileData = fileData + lines // Append each line to the fileData
    }
    bufferedSource.close()      // Close the file
    fileData.split(", ").toList // Split the data into the correct strings
  }

  /** Generates a string from the schedule.
    *
    * @param includeFailures
    *   whether or not failures should be included
    * @param debugFormat
    *   if the string should be formatted as a valid Scala list
    * @return
    *   the stringified schedule
    */
  def scheduleToString(includeFailures: Boolean = true, debugFormat: Boolean = false): String =
    if !includeFailures then
      if debugFormat then "List(" + schedule.mkString(", ") + ")"
      else schedule.mkString(", ")
    else
      val sb = new StringBuilder(schedule.size * 10) // Set capacity for less resizing
      if debugFormat then sb.append("List(")
      var i    = 0
      val iter = schedule.iterator
      while (iter.hasNext) {
        val id = iter.next()
        val value =
          // We append failure information if it exists, otherwise we keep the schedule as is
          failureMapping.get(i) match {
            case Some(failureSchedule) =>
              id + "|" + failureSchedule.iterator.map(b => if b then '1' else '0').mkString(".")
            case None => id
          }
        if debugFormat then sb.append("\"" + value + "\"")
        else sb.append(value)
        if iter.hasNext then sb.append(", ")
        i += 1
      }
      if debugFormat then sb.append(")")
      sb.toString()

  def writeSchedule(fileName: String = "", id: String = ""): Unit = {
    if debug then println("Writing schedule to file")

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
    fileWriter.write(scheduleToString(recordFailureInjections)) // Write the task ids seperated by ", "
    fileWriter.close()                                          // Close the file writer

    // Switch back the history schedule as it was before
    if !done then schedule = schedule.reverse
  }

  /** Run McCCT on a function for a set number of iterations.
    *
    * @param func
    *   the function to run
    * @param iters
    *   the number of times to run the function
    * @param assertion
    *   an assertion that should hold each execution
    * @param alg
    *   the schedule exploration algorithm
    * @param shouldPrint
    *   if scheduler information should be printed
    * @param sequential
    *   if the scheduler should perform tasks sequentially
    * @param failureAlg
    *   the failure exploration algorithm
    */
  def run[T](
      func: => T,
      iters: Int = 1,
      assertion: T => Boolean = (_: T) => true,
      alg: ExplorationAlgorithm = RandomWalk,
      shouldPrint: Boolean = false,
      sequential: Boolean = false,
      failureAlg: FailureExplorationAlgorithm = NeverInject
  ): Unit = {
    (1 to iters).foreach(_ =>
      start(alg, shouldPrint, sequential, failureAlg)
      val res = func
      awaitTermination()
      assert(assertion(res))
      failureAlg.newIter()
    )
  }

}
