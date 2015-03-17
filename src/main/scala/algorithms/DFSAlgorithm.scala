package algorithms

import algorithms.types.{DFSData, AlgorithmData}
import main.scala.common.SatMapReduceHelper
import main.scala.domain.Formula
import main.scala.utils.{SatLoggingUtils, ISatCallback}
import org.apache.hadoop.io.{Text, LongWritable}

/**
 * Trait that has a DFS implementation used in the mappers
 * Created by mbarreto on 3/3/15.
 */
object DFSAlgorithm extends AbstractAlgorithm[String, (Int, Int)] with SatLoggingUtils {
  /**
   * Applies the algorithm given the imputs and calls @callback when a solution
   * is found.
   * Returns (solutions_found, prunned)
   * @param callback is called when a solution is found
   * @param fixed currently fixed variables
   * @param selected currently selected variables
   */
  override def applyAlgorithm(algorithmData: AlgorithmData,
                              callback: ISatCallback[String]): (Int, Int) = {
    var dfsData : DFSData = algorithmData.asInstanceOf[DFSData];

    if (dfsData.depth == 0) {
      val satSubproblem = SatMapReduceHelper.createSatString(dfsData.fixed ++ dfsData.selected)
      callback.apply(satSubproblem)
      return (1, 0)
    } else {
      //Still not searched deep enough
      var subsolsFound = 0
      var pruned = 0;
      val fixedSubproblem = dfsData.fixed ++ dfsData.selected
      //select one literal to fix.
      val l = selectLiteral(dfsData.formula, fixedSubproblem);
      if (l != 0) {
        //recursive part..
        val subproblem = fixedSubproblem ++ Set(l)
        if (!evaluateSubproblem(dfsData.formula, subproblem)) {
          var clauses = dfsData.formula.getFalseClauses(subproblem)
          //prune => do not search in this branch.
          pruned = pruned + 1;
        } else {
          dfsData.selected = dfsData.selected ++ Set(l)
          dfsData.depth = dfsData.depth -1
          var ij = applyAlgorithm(dfsData, callback);
          subsolsFound = subsolsFound + ij._1
          pruned = pruned + ij._2
        }
        val subproblemPositive = fixedSubproblem ++ Set(-l)
        log.debug(s"Selected -$l, subproblem: $subproblemPositive")
        if (!evaluateSubproblem(dfsData.formula, subproblemPositive)) {
          val clauses = dfsData.formula.getFalseClauses(subproblemPositive)
          log.debug(s"Problem instance $subproblemPositive makes the following clauses false:")
          clauses.foreach(c => log.debug(s"clause: ${c.literals} ${c.id}"))

          //prune => do not search in this branch.
          pruned = pruned + 1
        } else {
          dfsData.selected = dfsData.selected ++ Set(-l)
          dfsData.depth = dfsData.depth - 1
          var ij = applyAlgorithm(dfsData, callback);
          subsolsFound = subsolsFound + ij._1
          pruned = pruned + ij._2
        }
      }else{
        //couldn't retrieve a literal => all must be set
        if ((dfsData.fixed ++ dfsData.selected).size == dfsData.formula.n){
          //all literals set!, check if its a solution:
          val satSelectedLiterals = SatMapReduceHelper.createSatString(dfsData.fixed ++ dfsData.selected)
          if (dfsData.formula.isSatisfasiable(dfsData.fixed ++ dfsData.selected, log)) {
            //solution found ==> call callback!
            val satSelectedLiterals = SatMapReduceHelper.createSatString(dfsData.fixed ++ dfsData.selected)
            callback.apply(satSelectedLiterals)
            //            context.write(new LongWritable(Thread.currentThread().getId), new Text(satSelectedLiterals));
            //            saveToHBaseSolFound(satSelectedLiterals, satProblem, startTime)
            subsolsFound = 1;
            pruned = 0;
          }
        }
      }
      return (subsolsFound, pruned);
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
