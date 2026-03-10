package mccct

trait ExplorationAlgorithm:
  def getNext(readyTasks: List[Controller]): Option[List[Controller]]

  def prepareNext(taskHistory: List[String]): Unit

object FifoAlgorithm extends ExplorationAlgorithm:
  def getNext(readyTasks: List[Controller]): Option[List[Controller]] =
    if readyTasks.length == 1 then Some(readyTasks)
    else readyTasks.headOption.map(List(_))

  def prepareNext(taskHistory: List[String]): Unit = {}

object NoopAlgorithm extends ExplorationAlgorithm:
  def getNext(readyTasks: List[Controller]): Option[List[Controller]] = Some(readyTasks)

  def prepareNext(taskHistory: List[String]): Unit = {}

object RandomWalk extends ExplorationAlgorithm:
  def getNext(readyTasks: List[Controller]): Option[List[Controller]] =
    if readyTasks.length == 1 then Some(readyTasks)
    else {
      val shuffled = util.Random.shuffle(readyTasks)
      Some(List(shuffled.head))
    }

  def prepareNext(taskHistory: List[String]): Unit = {}

class FixedSchedule(var targetSchedule: List[String]) extends ExplorationAlgorithm:
  def getNext(readyTasks: List[Controller]): Option[List[Controller]] = {
    targetSchedule.headOption match // Take the id of the task we want to execute.
      case Some(item) =>
        // From the schedule head we extract what controller to run (and a potential failure schedule).
        // For example: "1.1.|0.1" => ctrl = "1.1.", failures = "0.1"
        val Array(ctrl, failures) =
          item.split("\\|", 2) match {
            case Array(c, f) => Array(c, f)
            case Array(c)    => Array(c, "")
          }

        val target = readyTasks.filter(c => c.id.getId() == ctrl)
        if target.isEmpty then return None
        targetSchedule = targetSchedule.tail // Remove head from schedule

        val selectedController = target.head

        // We add the failure schedule to the controller if it exists
        val failureSchedule =
          if failures.isEmpty then Vector()
          else failures.split('.').toVector.map(_ == "1")
        if failureSchedule.nonEmpty then selectedController.setNextFailureSchedule(failureSchedule)

        Some(List(selectedController)) // Take target task and control
      case None =>
        None
  }

  def prepareNext(taskHistory: List[String]): Unit = {}

  def hasNext(): Boolean = true
