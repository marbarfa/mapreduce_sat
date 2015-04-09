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
object DFSAlgorithm extends AbstractAlgorithm[(List[Int], Int, Int)] with SatLoggingUtils {
  /**
   * Applies the algorithm given the imputs and calls @callback when a solution
   * is found.
   * Returns (solutions_found, prunned)
   */
  override def applyAlgorithm(algorithmData: AlgorithmData): (List[Int], Int, Int) = {
    var dfsData : DFSData = algorithmData.asInstanceOf[DFSData];

    if (dfsData.depth == 0) {
      return (dfsData.fixed ++ dfsData.selected, 1, 0)
    } else {
      //Still not searched deep enough
      var subsolsFound = 0
      var pruned = 0;
      var newFixed = List[Int]()

      val fixedSubproblem = dfsData.fixed ++ dfsData.selected
      //select one literal to fix.
      val l = selectLiteral(dfsData.formula, fixedSubproblem)
      if (l != 0) {
        //recursive part..
        val subproblem = fixedSubproblem ++ Set(l)
        val subproblemPositive = fixedSubproblem ++ Set(-l)

        val evaluateAssignment = (assignment : List[Int]) => {
          if (!evaluateSubproblem(dfsData.formula, assignment)) {
            //prune => do not search in this branch.
            pruned = pruned + 1
          } else {
            dfsData.selected = dfsData.selected ++ Set(l)
            dfsData.depth = dfsData.depth -1
            val ij = applyAlgorithm(dfsData)
            newFixed = newFixed ++ ij._1
            subsolsFound = subsolsFound + ij._2
            pruned = pruned + ij._3
          }
        }

        evaluateAssignment(subproblem)
        evaluateAssignment(subproblemPositive)

      }else{
        //couldn't retrieve a literal => all must be set
        var fixedLits = dfsData.fixed ++ dfsData.selected
        if (fixedLits.size == dfsData.formula.n){
          //all literals set!, check if its a solution:
          if (dfsData.formula.isSatisfasiable(fixedLits, log)) {
            //solution found ==> call callback!
            newFixed = fixedLits
            subsolsFound = 1
            pruned = 0
            // callback.apply(satSelectedLiterals)
            // context.write(new LongWritable(Thread.currentThread().getId), new Text(satSelectedLiterals));
            // saveToHBaseSolFound(satSelectedLiterals, satProblem, startTime)
          }
        }
      }
      return (newFixed, subsolsFound, pruned);
    }
  }

  private def evaluateSubproblem(formula: Formula, subproblem: List[Int]): Boolean = {
      return formula.isSatisfasiable(subproblem, log)
  }

  private def selectLiteral(formula: Formula, vars: List[Int]): Int = {
    //iterate the literals by order of appearence in clauses.
    formula
      .getLiteralsInOrder()
      .slice(vars.size - 1, formula.n)
      .foreach(x => {
      if (!vars.contains(x) && !vars.contains(-x)) {
        return x;
      }
    })
    return 0;
  }


}
