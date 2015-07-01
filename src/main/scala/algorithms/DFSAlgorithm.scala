package algorithms

import algorithms.types.{DFSData, AlgorithmData}
import common.SatMapReduceHelper
import domain.Formula
import utils.{SatLoggingUtils, ISatCallback}
import org.apache.hadoop.io.{Text, LongWritable}

/**
 * Trait that has a DFS implementation used in the mappers
 * Created by mbarreto on 3/3/15.
 */
object DFSAlgorithm extends AbstractAlgorithm[Unit] with SatLoggingUtils {

  /**
   * Applies the algorithm given the imputs and calls @callback when a solution
   * is found.
   * Returns (fixed_literals, solutions_found, prunned)
   */
  override def applyAlgorithm(algorithmData: AlgorithmData) : Unit = {
    val dfsData : DFSData = algorithmData.asInstanceOf[DFSData];

    if (dfsData.depth == 0) {
      val selected = algorithmData.fixed ++ dfsData.selected
      dfsData.possibleSolutions = dfsData.possibleSolutions ++ List(selected)
    } else {
      //Still not searched deep enough
      var subsolsFound = 0
      var pruned = 0;

      //select one literal to fix.
      val l = selectLiteral(dfsData.formula, dfsData.fixed ++ dfsData.selected)
      if (l != 0) {
        //recursive part..

        //FUNCTION TO EVALUATE ASSIGNMENT in DFS ALGORITHM.
        val evaluateAssignment = (lit : Int) => {
          var possibleSolution = dfsData.fixed ++ dfsData.selected ++ List(lit);
          if (!evaluateSubproblem(dfsData.formula, possibleSolution)) {
            //prune => do not search in this branch.
            pruned = pruned + 1
          } else {
            val dfsDataRec = new DFSData(dfsData.fixed,
              dfsData.selected ++ List(lit),
              dfsData.depth-1,
              dfsData.possibleSolutions,
              dfsData.formula);

            applyAlgorithm(dfsDataRec)

            dfsData.possibleSolutions = dfsDataRec.possibleSolutions
          }
        }

        evaluateAssignment(l)
        evaluateAssignment(-l)

      }else{
        var possibleSol = algorithmData.fixed ++ dfsData.selected
        //couldn't retrieve a literal => all must be set
        if (possibleSol.size >= dfsData.formula.n){
          //all literals set!, check if its a solution:
          if (dfsData.formula.isSatisfasiable(possibleSol)) {
            //solution found ==> call callback!
            dfsData.possibleSolutions = dfsData.possibleSolutions ++ List(possibleSol)
          }
        }
      }
    }
  }

  private def evaluateSubproblem(formula: Formula, subproblem: List[Int]): Boolean = {
      return formula.isSatisfasiable(subproblem)
  }

  private def selectLiteral(formula: Formula, vars: List[Int]): Int = {
    //iterate the literals by order of appearence in clauses.
//    for(x <- 1 to formula.n){
//      if (!vars.contains(x) && !vars.contains(-x)) {
//        return x;
//      }
//    }
    formula
      .getLiteralsInOrder()
      .foreach(x => {
      if (!vars.contains(x) && !vars.contains(-x)) {
        return x;
      }
    })
    return 0;
  }


}
