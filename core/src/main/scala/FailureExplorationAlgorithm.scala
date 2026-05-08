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
class RandomlyInject(chance: Double, bound: Int = -1) extends FailureExplorationAlgorithm:
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

/** Algorithm that injects the failures with the given ids.
  *
  * @param ids
  *   the ids that should be injected
  * @param default
  *   failure algorithm to apply if failure has been injected previously
  * @param bound
  *   the maximum number of failures injected in one iteration
  */
class InjectOnId(ids: Set[Int], default: FailureExplorationAlgorithm = NeverInject) extends FailureExplorationAlgorithm:
  private var encounteredFailurePoints: Set[Int] = Set()
  def shouldInject(id: Int): Boolean =
    // We check if it is the id we want and that we have not triggered it previously
    if ids(id) && !encounteredFailurePoints(id) then
      encounteredFailurePoints += id
      true
    else
      default.shouldInject(id)
  def newIter(): Unit = encounteredFailurePoints = Set()
