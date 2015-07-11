package hadoop

import java.util.Date

import algorithms.{SchoningAlgorithm, DFSAlgorithm}
import algorithms.types.{SchoningData, DFSData}
import common.{SatMapReduceConstants, SatMapReduceHelper}
import domain.{Formula, Clause}
import utils.{HBaseHelper, SatLoggingUtils, ConvertionHelper, CacheHelper}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper


/**
 *
 * HBase table "var_tables" has only one column: LITERAL_COMBINATION | CLAUSE
 * If variables (x1 = 0, x5=1, x3=0  make clause 3 false then there will be a registry in HBase like:
 * "1:0 3:0 5:1" --> "3"
 *
 *
 * Created by marbarfa on 1/13/14.
 */
class SatMapReduceMapper extends Mapper[LongWritable, Text, LongWritable, Text] with ConvertionHelper
with SatLoggingUtils with HBaseHelper {

  var formula: Formula = _
  var depth: Int = _
  var iteration: Int = _
  var satProblem: String = _
  var solFound: (String, Long) = null
  var startTime: Long = _
  type Context = Mapper[LongWritable, Text, LongWritable, Text]#Context

  protected override def setup(context: Context) {
    // retrieve 3SAT instance.
    satProblem = context.getConfiguration.get("problem_path");
    depth = context.getConfiguration.get(SatMapReduceConstants.config.depth).toInt
    iteration = context.getConfiguration.get(SatMapReduceConstants.config.iteration).toInt
    startTime = context.getConfiguration.getLong("start_miliseconds", 0);

    initHTable();
    solFound = retrieveSolution(satProblem)
  }

  protected override def cleanup(context: Context) {
    formula = null;
    table = null;
  }


  /**
   * The mapper key will be
   * @param key line offset
   * @param value subproblem definition like: (1 -2 3 4) --> fixed values: 1=true and 2=false and
   *              2 variables: (3=false,4=false)
   * @param context
   */
  override def map(key: LongWritable, value: Text, context: Context) {
    val start = System.currentTimeMillis();
    if (solFound == null) {
      val fixed: List[Int] = SatMapReduceHelper.parseInstanceDef(value.toString)
      log.info(s"[Iteration $iteration|fixed: ${fixed.size} Mapper value: ${value.toString}, fixed: ${fixed.toString()}")
      if (fixed.size > 0) {
        //Apply DFS algorithm
        formula = retrieveFormula(SatMapReduceHelper.createSatString(fixed), satProblem)
        var dfsData = new DFSData(fixed, List[Int](), depth, List[List[Int]](), formula)

        DFSAlgorithm.applyAlgorithm(dfsData)
        if (dfsData.possibleSolutions.size > 0)
        for (possibleSol <- dfsData.possibleSolutions) {
          if (possibleSol.size < formula.n) {
            //not all literals set ==> do Schöning algorithm
            var schoningData = new SchoningData(possibleSol, formula)
            var schResult = SchoningAlgorithm.applyAlgorithm(schoningData)
            if (schResult != null) {
              // Solution Found
              saveToHBaseSolFound(SatMapReduceHelper.createSatString(schResult), satProblem, start)
            } else {
              var literalDef = SatMapReduceHelper.createSatString(possibleSol);

              context.write(key, new Text(literalDef))
            }
          } else if (dfsData.formula.isSatisfasiable(possibleSol)) {
            // Solution Found
            saveToHBaseSolFound(SatMapReduceHelper.createSatString(possibleSol), satProblem, start)
          }
        }
      }
    }

  }





}
