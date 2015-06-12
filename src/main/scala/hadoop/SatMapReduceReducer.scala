package hadoop

import java.lang
import algorithms.{SchoningAlgorithm, DFSAlgorithm, PureLiteralEliminationAlgorithm, UnitPropagationAlgorithm}
import algorithms.types.{SchoningData, DFSData, AlgorithmData}
import enums.EnumMRCounters
import common.{SatMapReduceConstants, SatMapReduceHelper}
import domain.Formula
import utils.{HBaseHelper, SatLoggingUtils, ConvertionHelper, CacheHelper}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConverters._

/**
 * Created by marbarfa on 1/13/14.
 */
class SatMapReduceReducer extends Reducer[LongWritable, Text, NullWritable, Text] with ConvertionHelper with SatLoggingUtils
with HBaseHelper {

  var formula: Formula = _
  var startTime: Long = _
  var iteration: Int = _
  var fixedLiteralsNumber: Int = _
  var satProblem: String = _
  var startupTime: Long = _
  var solFound: Boolean = _
  var depth: Int = _
  type Context = Reducer[LongWritable, Text, NullWritable, Text]#Context

  override def setup(context: Context) {
    // retrieve 3SAT instance.
    satProblem = context.getConfiguration.get("problem_path");
    depth = context.getConfiguration.get(SatMapReduceConstants.config.depth).toInt
    iteration = context.getConfiguration.getInt("iteration", 1);
    startTime = context.getConfiguration.getLong("start_miliseconds", 0);
    fixedLiteralsNumber = context.getConfiguration.getInt("fixed_literals", 0);

    initHTable()
    solFound = retrieveSolution(satProblem)
  }

  protected override def cleanup(context: Context) {
    formula = null;
    table = null;
  }

  def saveSolution(solutionMap: List[Int]) = {
    log.debug(s"Saving solution ${solutionMap.toString()} to ${SatMapReduceConstants.sat_solution_path}")
    //save solution to file.
    val solutionString = SatMapReduceHelper.createSatString(solutionMap);

    SatMapReduceHelper.saveStringToFile(
      s"""
         |##########################################################################
         |Solution found in ${(System.currentTimeMillis() - startTime) / 1000} seconds
         |Solution found in interation $iteration.
         |SAT Problem: $satProblem
         |--------------------------------------------------------------------------
         |Solution: $solutionString
         |##########################################################################
      """.stripMargin,
      s"${SatMapReduceConstants.sat_solution_path}-${satProblem.split("/").last}-${startTime.toString}", true);
  }

  override def reduce(key: LongWritable, values: lang.Iterable[Text], context: Context) {
    if (!solFound) {
      values.asScala.foreach(v => {
        val literalDefinition = SatMapReduceHelper.parseInstanceDef(v.toString.trim)
        formula = retrieveFormula(v.toString.trim, satProblem)

        if (formula.n <= literalDefinition.size) {
          //All literals set
          if (formula.isSatisfasiable(literalDefinition, log)) {
            log.info(s"Solution found = ${literalDefinition.toString()}!!!")
            saveSolution(literalDefinition);
            context.getCounter(EnumMRCounters.SOLUTIONS).increment(1);
          } else {
            log.info(s"Solution ${literalDefinition.toString()} not satisfasiable!")
          }
        } else {
          //Solution not found yet

          //apply unit propagation
          var data = new AlgorithmData(literalDefinition, formula)
          var unitPropagationResult = UnitPropagationAlgorithm.applyAlgorithm(data)
          if (unitPropagationResult._2.size >= formula.n) {
            //all literals set after unit propagation!!
            if (unitPropagationResult._1.isSatisfasiable(unitPropagationResult._2, log)) {
              //Solution found!!!
              doSolutionFound(unitPropagationResult._2, context)
            } else {
              //apply pure literal elimination
              data = new AlgorithmData(unitPropagationResult._2, unitPropagationResult._1)
              var pureLiteralElim = PureLiteralEliminationAlgorithm.applyAlgorithm(data)

              if (pureLiteralElim == null){
                // the assignment makes the formula false!
                return;
              }

              if (pureLiteralElim._2.size >= formula.n) {
                if (pureLiteralElim._1.isSatisfasiable(pureLiteralElim._2, log)) {
                  //Solution found!!!
                  doSolutionFound(unitPropagationResult._2, context)
                } else {
                  //apply dfs
                  var dfsData = new DFSData(pureLiteralElim._2, List[Int](), depth, pureLiteralElim._1)
                  var resData = DFSAlgorithm.applyAlgorithm(dfsData)
                  if (resData._1.size >= formula.n) {
                    log.info(
                      s"""
                         |##################    Mapper Stats   ###################
                         |fixed: ${resData._1.size}
                          |Sols found : ${resData._2}|Pruned: ${resData._3}
                        """.stripMargin)
                    if (pureLiteralElim._1.isSatisfasiable(resData._1, log)) {
                      //SolutionFound!!
                      doSolutionFound(resData._1, context)
                    } else {
                      log.info(s"Solution ${resData._1.toString()} not satisfasiable!")
                    }
                  } else {
                    //apply schoning
                    var schoningData = new SchoningData(resData._1, pureLiteralElim._1)
                    var schResult = SchoningAlgorithm.applyAlgorithm(schoningData)
                    if (schResult != null) {
                      doSolutionFound(schResult, context)
                    } else {
                      //output the partial solution
                      context.getCounter(EnumMRCounters.SUBPROBLEMS).increment(1);
                      //still a partial solution
                      var ps = SatMapReduceHelper.createSatString(pureLiteralElim._2)
                      log.info(s"Partial solution: ${ps} of $literalDefinition")
                      context.write(NullWritable.get(), new Text(ps.getBytes))

                      //Save new formula!
                      saveToHBaseFormula(ps, pureLiteralElim._1)
                    }
                  }
                }
              }
            }
          }
        }
      })
    }
  }

  private def doSolutionFound(literalDef: List[Int], context: Context) = {
    log.info(s"Solution found with UP = ${literalDef.toString()}!!!")
    saveToHBaseSolFound(SatMapReduceHelper.createSatString(literalDef), satProblem, startTime)
//    saveSolution(literalDef);
    context.getCounter(EnumMRCounters.SOLUTIONS).increment(1);
  }


}
