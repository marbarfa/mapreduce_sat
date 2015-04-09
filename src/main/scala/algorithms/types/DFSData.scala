package algorithms.types

import domain.Formula

/**
 * Created by mbarreto on 3/10/15.
 */
class DFSData(override var fixed: List[Int],
              var selected: List[Int],
              var depth: Int,
              override var formula: Formula) extends AlgorithmData(fixed, formula) {
}
