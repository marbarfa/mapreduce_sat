package algorithms

import algorithms.types.AlgorithmData
import main.scala.utils.ISatCallback

/**
 * Created by mbarreto on 3/3/15.
 */
trait AbstractAlgorithm[T,R] {

  /**
   * Applies the algorithm given the imputs and calls @callback when a solution
   * is found.
   * Returns (solutions_found, prunned)
   * @param callback is called when a solution is found
   * @param fixed currently fixed variables
   * @param selected currently selected variables
   */
  def applyAlgorithm(algorithmData: AlgorithmData,
      callback: ISatCallback[T]): R

}
