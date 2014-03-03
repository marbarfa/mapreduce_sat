package scala.common

import scala.domain.Variable

/**
 * Created by marbarfa on 3/3/14.
 */
object SatMapReduceHelper {

  /**
   * This method returns some random (or not) variables to be used to split the problem
   * among the mappers.
   * eg: if input is (1,2) and there are 10 variables, the return could return List(3,4)
   * @param fixedVars
   * @param n
   * @return
   */
  def generateProblemSplit(fixedVars: List[Variable], n: Int, amount : Int): List[Variable] = {
    var variables: List[Variable] = _
    for (i <- 0 until n) {
      var possibleVar = new Variable(i)
      if (!fixedVars.contains(possibleVar)) {
        variables = possibleVar :: variables;
      }
      if (variables.size >= amount){
        return variables;
      }
    }
  }

}
