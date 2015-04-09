package algorithms

import algorithms.types.AlgorithmData

/**
 * Created by mbarreto on 3/3/15.
 */
trait AbstractAlgorithm[R] {

  /**
   * Applies the algorithm given the imputs and calls @callback when a solution
   * is found.
   * Returns (solutions_found, prunned)
   */
  def applyAlgorithm(algorithmData: AlgorithmData) : R

}
