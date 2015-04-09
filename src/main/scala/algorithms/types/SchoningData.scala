package algorithms.types

import domain.Formula

/**
 * Created by mbarreto on 3/10/15.
 */
class SchoningData(override var fixed: List[Int],
                   override var formula: Formula) extends AlgorithmData(fixed, formula) {

}
