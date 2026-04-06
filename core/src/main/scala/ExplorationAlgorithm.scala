package mccct

trait ExplorationAlgorithm:
  def getNext(readyTasks: Vector[Controller]): Option[Vector[Controller]]

  def prepareNext(taskHistory: Vector[String]): Unit

object FifoAlgorithm extends ExplorationAlgorithm:
  def getNext(readyTasks: Vector[Controller]): Option[Vector[Controller]] =
    if readyTasks.length == 1 then Some(readyTasks)
    else readyTasks.headOption.map(Vector(_))

  def prepareNext(taskHistory: Vector[String]): Unit = {}

object NoopAlgorithm extends ExplorationAlgorithm:
  def getNext(readyTasks: Vector[Controller]): Option[Vector[Controller]] = Some(readyTasks)

  def prepareNext(taskHistory: Vector[String]): Unit = {}

object RandomWalk extends ExplorationAlgorithm:
  def getNext(readyTasks: Vector[Controller]): Option[Vector[Controller]] =
    if readyTasks.length == 1 then Some(readyTasks)
    else {
      val shuffled = util.Random.shuffle(readyTasks)
      Some(Vector(shuffled.head))
    }

  def prepareNext(taskHistory: Vector[String]): Unit = {}

class FixedSchedule(var targetSchedule: List[String]) extends ExplorationAlgorithm:
  def getNext(readyTasks: Vector[Controller]): Option[Vector[Controller]] = {
    targetSchedule.headOption match // Take the id of the task we want to execute.
      case Some(ctrl) =>
        val target = readyTasks.filter(c => c.id.getId() == ctrl)
        if target.isEmpty then return None
        targetSchedule = targetSchedule.tail // Remove head from schedule
        Some(Vector(target.head))              // Take target task and control
      case None =>
        None
  }

  def prepareNext(taskHistory: Vector[String]): Unit = {}

  def hasNext(): Boolean = true
