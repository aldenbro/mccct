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

class RegressionSchedule(var targetSchedule: List[String]) extends ExplorationAlgorithm:
  def getNext(readyTasks: Vector[Controller]): Option[Vector[Controller]] =
    targetSchedule.headOption.flatMap { targetId =>
      // Find controller matching the schedule head
      val scheduledCtrl = readyTasks.find(_.id.getId() == targetId)

      scheduledCtrl match
        case None =>
          val childId = endChildId(targetId)
          // If the "0." child exists, then that means we could finish the current target for the schedule
          // In these cases start the next one, and remove original
          readyTasks.find(_.id.getId() == childId) match
            case Some(childCtrl) =>
              targetSchedule = targetSchedule.tail
              getNext(readyTasks)

            case None =>
              None

        case Some(ctrl) =>
          val chosen =
            if !ctrl.isWaiting then ctrl
            else
              val lock = ctrl.waitingFor
              if ctrl.isWaiting then
                targetSchedule =
                  targetId +: targetSchedule // Waiting for a lock required one of the original instances of this id
                ctrl.resetWaiting
              // Try to find a controller holding the lock, else we can choose this one
              readyTasks.find(_.holdsLock(lock)).getOrElse(ctrl)
          targetSchedule = removeFirst(targetSchedule, chosen)

          Some(Vector(chosen))
    }

  private def endChildId(id: String): String =
    s"${id}0."

  private def removeFirst(list: List[String], elem: Controller): List[String] = {
    val (before, after) = list.span(_ != elem.id.getId())
    before ++ after.drop(1)
  }

  def prepareNext(taskHistory: Vector[String]): Unit = {}

  def hasNext(): Boolean = true
