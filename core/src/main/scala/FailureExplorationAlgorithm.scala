package mccct

trait FailureExplorationAlgorithm:
  def shouldInject(id: Int): Boolean

  def newIter(): Unit

object NeverInject extends FailureExplorationAlgorithm:
  def shouldInject(id: Int): Boolean = false
  def newIter(): Unit                = ()

object AlwaysInject extends FailureExplorationAlgorithm:
  def shouldInject(id: Int): Boolean = true
  def newIter(): Unit                = ()

/** Algorithm that injects failures randomly.
  *
  * @param chance
  *   the chance to inject failure [0, 1]
  * @param bound
  *   the maximum number of failures injected in one iteration
  */
class InjectRandomly(chance: Double, bound: Int = -1) extends FailureExplorationAlgorithm:
  private var failuresInjected       = 0
  def shouldInject(id: Int): Boolean =
    if bound >= 0 && failuresInjected >= bound then false
    else
      val shouldInject = scala.util.Random.nextDouble() < chance
      if shouldInject then failuresInjected += 1
      shouldInject
  def newIter(): Unit = failuresInjected = 0

/** Algorithm that injects failures that has not been injected previously.
  *
  * @param default
  *   failure algorithm to apply if failure has been injected previously
  * @param bound
  *   the maximum number of failures injected in one iteration
  */
class InjectOnNew(default: FailureExplorationAlgorithm = NeverInject, bound: Int = -1)
    extends FailureExplorationAlgorithm:
  private var failuresInjected                   = 0
  private var encounteredFailurePoints: Set[Int] = Set()
  def shouldInject(id: Int): Boolean             =
    if bound >= 0 && failuresInjected >= bound then false
    else if !encounteredFailurePoints(id) then
      encounteredFailurePoints += id
      failuresInjected += 1
      true
    else
      val shouldInject = default.shouldInject(id)
      if shouldInject then failuresInjected += 1
      shouldInject
  def newIter(): Unit = failuresInjected = 0
