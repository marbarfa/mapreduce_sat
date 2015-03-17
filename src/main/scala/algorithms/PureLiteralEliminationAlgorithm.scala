package algorithms

import main.scala.domain.Formula
import main.scala.utils.ISatCallback

/**
 * Created by mbarreto on 3/8/15.
 */
object PureLiteralEliminationAlgorithm extends AbstractAlgorithm{

  /**
   * Applies the algorithm given the imputs and calls @callback when a solution
   * is found.
   * Returns (solutions_found, prunned)
   * @param callback is called when a solution is found
   * @param fixed currently fixed variables
   * @param selected currently selected variables
   */
  override def applyAlgorithm(fixed: List[Int], selected: List[Int], depth: Int, formula: Formula, callback: ISatCallback): (Int, Int) = ???
}
