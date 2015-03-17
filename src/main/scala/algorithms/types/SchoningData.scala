package algorithms.types

import main.scala.domain.Formula

/**
 * Created by mbarreto on 3/10/15.
 */
class SchoningData(override var fixed: List[Int],
                   var selected: List[Int],
                   var depth: Int,
                   override var formula: Formula) extends AlgorithmData(fixed, formula) {

}
