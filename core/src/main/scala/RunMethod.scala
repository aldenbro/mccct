package mccct

import gears.async.Async
import scala.collection.mutable.Queue

trait RunMethod:
  /** @param method
    *   the method to run using the method
    * @return
    *   the return value of the method (if it returned something) and if the method can continue
    */
  def runIteration[T](method: (gears.async.Async, Controller) ?=> T): (Option[T], Boolean)
  def reset(): Unit

class BasicRun(
    alg: ExplorationAlgorithm = RandomWalk,
    failureAlg: FailureExplorationAlgorithm = NeverInject,
    sequential: Boolean = true
) extends RunMethod:
  def runIteration[T](method: (gears.async.Async, Controller) ?=> T): (Option[T], Boolean) = {
    var res: Option[T] = None
    Scheduler(alg = alg, failureAlg = failureAlg, includeTaskEndings = false, sequential = sequential) {
      res = Some(method)
    }
    (res, true)
  }
  def reset(): Unit = {}

class FailureExploration(
    defaultAlg: ExplorationAlgorithm = RandomWalk,
    injectionBound: Int = -1,
    cctIterations: Int = 1,
    sequential: Boolean = true
) extends RunMethod:
  case class Config(
      alg: ExplorationAlgorithm,
      failureAlg: FailureExplorationAlgorithm,
      injectionCount: Int
  )

  var isFirstIteration  = true
  var cctIterationsLeft = 0
  var stack             = List.empty[Config]

  var config: Config = getNextConfig()

  def runIteration[T](method: (Async, Controller) ?=> T): (Option[T], Boolean) = {
    // If we have run out of cct iterations, get a new configuration
    if cctIterationsLeft <= 0 then config = getNextConfig()
    // We run the method using the information obtained above
    var res: Option[T] = None
    Scheduler(alg = config.alg, failureAlg = config.failureAlg, includeTaskEndings = false, sequential = sequential) {
      res = Some(method)
    }
    cctIterationsLeft -= 1
    // If we have no cct iterations left and have not reached the injection bound, register encountered failure points
    if cctIterationsLeft <= 0 && (config.injectionCount < injectionBound || injectionBound == -1) then {
      Scheduler
        .getFailurePointFirstEncountered()
        .foreach(point =>
          val (idToInject, (scheduleCutoff, failureCutoff)) = point
          // We create a partial schedule up until the failure point we want to inject
          val partialSchedule = Scheduler.createPartialSchedule(Scheduler.getSchedule(), scheduleCutoff, failureCutoff)
          // We can then add a new configuration where we replay the schedule up until the point we want to inject
          stack = Config(
            FixedSchedule(partialSchedule, defaultAlg),
            InjectOnId(Set(idToInject)),
            config.injectionCount + 1
          ) :: stack
        )
    }
    // We return the result and if the method can still be used
    (res, stack.nonEmpty || cctIterationsLeft > 0)
  }

  def getNextConfig(): Config = {
    // Set how many times the new config should run for
    cctIterationsLeft = cctIterations
    if isFirstIteration
    // If it is the first iteration, we run without injecting any failures
    then {
      isFirstIteration = false
      Config(alg = defaultAlg, failureAlg = NeverInject, injectionCount = 0)
    }
    // Otherwise, we grab a created config from the stack
    else {
      stack match
        case config :: tail =>
          stack = tail
          config
        case Nil =>
          assert(false, "The contract of the method has been broken, previously returned that it could not continue.")
    }
  }

  def reset(): Unit = {
    var isFirstIteration  = true
    var cctIterationsLeft = 0
    var stack             = List.empty[Config]

    var config: Config = getNextConfig()
  }

class ImprovedFailureExploration(
    defaultAlg: ExplorationAlgorithm = RandomWalk,
    cctIterations: Int = 1,
    sequential: Boolean = true
) extends RunMethod:
  case class Config(
      alg: ExplorationAlgorithm,
      failureAlg: FailureExplorationAlgorithm
  )

  var isFirstIteration     = true
  var cctIterationsLeft    = 0
  var queue: Queue[Config] = Queue.empty

  // Needs to be after the previous fields
  var config: Config = getNextConfig()

  var seenIds: Set[Int] = Set.empty

  def runIteration[T](method: (Async, Controller) ?=> T): (Option[T], Boolean) = {
    // If we have run out of cct iterations, get a new configuration
    if cctIterationsLeft <= 0 then config = getNextConfig()

    // We run the method using the information obtained above
    var res: Option[T] = None
    Scheduler(alg = config.alg, failureAlg = config.failureAlg, includeTaskEndings = false, sequential = sequential) {
      res = Some(method)
    }
    cctIterationsLeft -= 1

    // Cleanup config so that it can be used as a new config next step
    config.alg match
      case a: FixedSchedule => a.reset()
      case _ => ()
    config.failureAlg.newIter()

    // For each failure point we add it to the queue if it has not been seen before
    Scheduler
      .getFailurePointFirstEncountered()
      .foreach(point =>
        val (idToInject, (scheduleCutoff, failureCutoff)) = point
        if !seenIds(idToInject) then {
          seenIds += idToInject
          // We create a partial schedule up until the failure point we want to inject
          val partialSchedule = Scheduler.createPartialSchedule(Scheduler.getSchedule(), scheduleCutoff, failureCutoff)
          // We can then add a new configuration where we replay the schedule up until the point we want to inject
          queue.enqueue(
            Config(
              FixedSchedule(partialSchedule, defaultAlg),
              InjectOnId(Set(idToInject))
            )
          )
        }
      )
    // We return the result and if the method can still be used
    (res, queue.nonEmpty || cctIterationsLeft > 0)
  }

  def getNextConfig(): Config = {
    // Set how many times the new config should run for
    cctIterationsLeft = cctIterations
    // If it is the first iteration, we run without injecting any failures
    if isFirstIteration then {
      isFirstIteration = false
      Config(alg = defaultAlg, failureAlg = NeverInject)
    }
    // Otherwise, we grab a created config from the queue
    else {
      assert(
        queue.nonEmpty,
        "The contract of the method has been broken, previously returned that it could not continue."
      )
      queue.dequeue()
    }
  }

  def reset(): Unit = {
    isFirstIteration  = true
    cctIterationsLeft = 0
    queue = Queue.empty

    config = getNextConfig()

    seenIds = Set.empty
  }
