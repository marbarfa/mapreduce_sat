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

  var startTime: Long = _
  var iteration: Int = _
  var fixedLiteralsNumber: Int = _
  var satProblem: String = _
  var startupTime: Long = _
  var solFound: Boolean = _
  var depth: Int = _
  type Context = Reducer[LongWritable, Text, NullWritable, Text]#Context

  private val NOT_SOLUTION = "SOLUTION_NOT_FOUND"
  private val NOT_ALL_FIXED = "NOTALLFIXED_NOT_FOUND"
  private val SOLUTION_FOUND = "SOLUTION_FOUND"


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
        log.info(s"[REDUCER] Received fixed literals: ${v.toString}")
        var literalDefinition: List[Int] = SatMapReduceHelper.parseInstanceDef(v.toString.trim)
        var formula: Formula = retrieveFormula(v.toString.trim, satProblem)

        if (formula.n <= literalDefinition.size) {
          //All literals set
          if (formula.isSatisfasiable(literalDefinition)) {
            log.info(s"[line 74] - Solution found = ${literalDefinition.toString()}!!!")
            saveSolution(literalDefinition);
            context.getCounter(EnumMRCounters.SOLUTIONS).increment(1);
          } else {
            log.info(s"Solution ${literalDefinition.toString()} not satisfasiable!")
          }
        } else {
          //apply unit propagation
          var data = new AlgorithmData(literalDefinition, formula);

          var upRes = applyUnitPropagation(formula, literalDefinition, context)
          var state = upRes._1
          formula = upRes._2
          literalDefinition = upRes._3

          if (!NOT_SOLUTION.equals(state)) {
            //APPLY PURE LITERAL ELIMINATION
            val (state2, formula2, literalDefinition2) = applyPureLiteralElimination(formula, literalDefinition, context)
            state = state2;
            formula = formula2;
            literalDefinition = literalDefinition2;

            if (!NOT_SOLUTION.equals(state)) {
              //APPLY DFS ALGORITHM.
              var dfsData = new DFSData(literalDefinition, List[Int](), depth, List[List[Int]](), formula)
              DFSAlgorithm.applyAlgorithm(dfsData)

              //For each possible Solution
              for (possibleSol <- dfsData.possibleSolutions) {

                if (!NOT_SOLUTION.equals(state)) {

                  //APPLY SCHONNING ALGORITHM.
                  val (state4, literalDefinition4) = applySchoning(dfsData.formula, possibleSol, context)

                  if (!SOLUTION_FOUND.equals(state)) {
                    context.getCounter(EnumMRCounters.SUBPROBLEMS).increment(1);
                    //still a partial solution
                    var ps = SatMapReduceHelper.createSatString(literalDefinition)
                    log.info(s"[MAPREDUCE SOL]Partial solution: ${ps} of $literalDefinition")
                    context.write(NullWritable.get(), new Text(ps.getBytes))

                    //Save new formula!
                    saveToHBaseFormula(ps, formula)
                  }
                }
              }
            }
          }
        }
      })
    }
  }

  /**
   * Applies the Schonning algorithm and returns the state and list of literals if a solution si found.
   * @param formula
   * @param literalDefinition
   * @param context
   * @return
   */
  private def applySchoning(formula: Formula, literalDefinition: List[Int], context: Context): (String, List[Int]) = {
    //apply schoning
    var schoningData = new SchoningData(literalDefinition, formula)
    var schResult = SchoningAlgorithm.applyAlgorithm(schoningData)
    if (schResult != null && schResult.size >= formula.n) {
      doSolutionFound(schResult, formula, context, s"Solution ${schResult.toString} with SCHONING")
      return (SOLUTION_FOUND, schResult)
    } else {
      return (NOT_ALL_FIXED, schResult)
    }
  }

  /**
   * Applies the Unit propagation algorithm to the formula.
   * @param formula the formula to receive
   * @param literalDefinition the current fixed set of literals.
   * @param context current MR context
   * @return returns triplet of (current state, new formula created, list of fixed literals)
   */
  private def applyUnitPropagation(formula: Formula, literalDefinition: List[Int], context: Context): (String, Formula, List[Int]) = {
    var data = new AlgorithmData(literalDefinition, formula)
    var unitPropagationResult = UnitPropagationAlgorithm.applyAlgorithm(data)
    log.info(s"Literals after UP ${unitPropagationResult._2.toString()}, " +
      s"formula: ${if (unitPropagationResult._1 == null) " NULL" else " NOT NULL"}")

    var status: String = ""

    if (unitPropagationResult._1 == null) {
      status = NOT_SOLUTION
    } else if (unitPropagationResult._2.size >= formula.n) {
      //all literals set after unit propagation!!
      if (unitPropagationResult._1.isSatisfasiable(unitPropagationResult._2)) {
        //Solution found!!!
        doSolutionFound(unitPropagationResult._2, unitPropagationResult._1, context, s"Solution ${unitPropagationResult._2.toString} with UP")
        status = SOLUTION_FOUND;
      } else {
        status = NOT_SOLUTION;
      }
    }
    return (status, unitPropagationResult._1, unitPropagationResult._2)
  }

  /**
   * Applies the pure literal elimination algorithm to the formula.
   * @param formula the formula to receive
   * @param literalDefinition the current fixed set of literals.
   * @param context current MR context
   * @return returns triplet of (current state, new formula created, list of fixed literals)
   */
  private def applyPureLiteralElimination(formula: Formula, literalDefinition: List[Int], context: Context): (String, Formula, List[Int]) = {
    //apply pure literal elimination
    var data = new AlgorithmData(literalDefinition, formula)
    var pureLiteralElim = PureLiteralEliminationAlgorithm.applyAlgorithm(data)

    var status = NOT_ALL_FIXED
    if (pureLiteralElim._1 == null) {
      // the assignment makes the formula false!
      status = NOT_SOLUTION
    } else if (pureLiteralElim._2.size >= formula.n) {
      if (pureLiteralElim._1.isSatisfasiable(pureLiteralElim._2)) {
        //Solution found!!!
        doSolutionFound(pureLiteralElim._2, pureLiteralElim._1, context, s"Solution ${pureLiteralElim._2.toString} with PLE")
        status = SOLUTION_FOUND
      } else {
        status = NOT_SOLUTION
      }
    }
    return (status, pureLiteralElim._1, pureLiteralElim._2)
  }


  private def doSolutionFound(literalDef: List[Int], formula: Formula, context: Context, textLog: String) = {
    log.info(s"SOLUTION FOUND!!: lits: ${literalDef.toString()}, n: ${formula.n}")
    if (literalDef.size < formula.n) {
      throw new RuntimeException(s"Solution with less literals set!!, lits: ${literalDef.toString()}, n: ${formula.n}")
    } else {
      log.info(s"$textLog")
      saveToHBaseSolFound(SatMapReduceHelper.createSatString(literalDef), satProblem, startTime)
      //    saveSolution(literalDef);
      context.getCounter(EnumMRCounters.SOLUTIONS).increment(1);
    }
  }


}
