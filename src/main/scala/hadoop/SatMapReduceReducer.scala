package main.scala.hadoop

import java.lang
import main.java.enums.EnumMRCounters
import main.scala.common.{SatMapReduceConstants, SatMapReduceHelper}
import main.scala.domain.Formula
import main.scala.utils.{HBaseHelper, SatLoggingUtils, ConvertionHelper, CacheHelper}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConverters._

/**
 * Created by marbarfa on 1/13/14.
 */
class SatMapReduceReducer extends Reducer[Text, Text, NullWritable, Text] with ConvertionHelper
with SatLoggingUtils with HBaseHelper {

  var formula: Formula = _
  var startTime: Long = _
  var iteration: Int = _
  var fixedLiteralsNumber: Int = _
  var satProblem: String = _

  override def setup(context: Reducer[Text, Text, NullWritable, Text]#Context) {
    // retrieve 3SAT instance.
    satProblem = context.getConfiguration.get("problem_path");
    if (formula == null) {
      formula = CacheHelper.sat_instance(satProblem)
    }

    iteration = context.getConfiguration.getInt("iteration", 1);
    startTime = context.getConfiguration.getLong("start_miliseconds", 0);
    fixedLiteralsNumber = context.getConfiguration.getInt("fixed_literals", 0);

    initHTable()
  }

  protected override def cleanup(context: Reducer[Text, Text, NullWritable, Text]#Context) {
    formula = null;
    table = null;
  }

  def saveSolution(solutionMap: List[Int]) = {
    log.debug(s"Saving solution ${solutionMap.toString()} to ${SatMapReduceConstants.sat_solution_path}")
    //save solution to file.
    var solutionString = SatMapReduceHelper.createSatString(solutionMap);

    SatMapReduceHelper.saveStringToFile(
      s"""
        | ##########################################################################
        | Solution found in ${(System.currentTimeMillis() - startTime) / 1000} seconds
        | Solution found in interation $iteration.
        | SAT Problem: $satProblem
        | --------------------------------------------------------------------------
        | Solution: $solutionString
        | ##########################################################################
      """.stripMargin,
      SatMapReduceConstants.sat_solution_path + satProblem, true);
  }

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, NullWritable, Text]#Context) {
    var fixedLiterals = 0
    var currentSelected = SatMapReduceHelper.parseInstanceDef(key.toString.trim)
    fixedLiterals = currentSelected.size + fixedLiteralsNumber;
    var fixed : List[String] = values.asScala.toList map (x => x.toString)
    fixed.foreach(v => {
//      var currentFixed = SatMapReduceHelper.parseInstanceDef(v.toString.trim)
      if (formula.n <= fixedLiterals) {
        var solFound = evaluateSolution(key.toString, fixed);
        if (solFound) {
          context.getCounter(EnumMRCounters.SOLUTIONS).increment(1);
        }
      }
    })
    if (formula.n > fixedLiterals) {
      context.write(NullWritable.get(), new Text(key))
      context.getCounter(EnumMRCounters.SUBPROBLEMS).increment(1l)
      //saving "found_key" -> "fixed1|fixed2|fixed3"
      saveToHBaseLiteralPath(key.toString.trim, fixed.foldLeft("")((acc, b) => s"$acc&${b.trim}").substring(1))
    }
  }

  /**
   * This method searches
   * @param key
   * @param values
   * @return retuns a literal definition of the solution found or null if no solution could be found.
   */
  private def evaluateSolution(key: String, values: List[String]): Boolean = {
    log.info(s"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    log.info(s"Evaluating solution to the problem..., searching for paths.")
    values.foreach(v => {
      log.info(s"Retriving path for ${v.toString}, key=${key.toString}")
      var paths = retrieveLiteralsPaths(v.toString)
      var keyLiterals = stringToIntSet(key);
      log.info(s"Retrieved path $paths")
      paths.foreach(literalCombination => {
        var sol = literalCombination ++ keyLiterals
        log.info(s"Possible solution: $sol")
        if (formula isSatisfasiable (sol,log)) {
          log.info(s"Solution found = ${sol.toString()}!!!")
          saveSolution(sol);
          return true;
        } else {
          log.info(s"Solution ${sol.toString()} not satisfasiable!")
        }
      })
    })
    log.info(s"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    return false;
  }


}
