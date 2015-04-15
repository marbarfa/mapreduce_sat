package algorithms.types

import domain.Formula

/**
 * Created by mbarreto on 3/10/15.
 */
class SchoningData(override val fixed: List[Int],
                   override val formula: Formula) extends AlgorithmData(fixed, formula) {

}
