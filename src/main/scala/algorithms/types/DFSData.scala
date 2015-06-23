package algorithms.types

import domain.Formula

/**
 * Created by mbarreto on 3/10/15.
 */
class DFSData(override val fixed: List[Int],
              var selected: List[Int],
              var depth: Int,
              var possibleSolutions : List[List[Int]],
              override val formula: Formula) extends AlgorithmData(fixed, formula) {
}
