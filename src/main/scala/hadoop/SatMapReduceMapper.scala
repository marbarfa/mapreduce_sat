package main.scala.hadoop

import java.util.Date

import main.scala.common.{SatMapReduceConstants, SatMapReduceHelper}
import main.scala.domain.{Formula, Clause}
import main.scala.utils.{HBaseHelper, SatLoggingUtils, ConvertionHelper, CacheHelper}
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
  var solFound: Boolean = _
  var startTime: Long = _
  type Context = Mapper[LongWritable, Text, LongWritable, Text]#Context

  protected override def setup(context: Context) {
    // retrieve 3SAT instance.
    if (formula == null) {
      formula = CacheHelper.sat_instance(context.getConfiguration.get(SatMapReduceConstants.config.problem_path))
    }

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
    if (!solFound) {
      val fixed: List[Int] = SatMapReduceHelper.parseInstanceDef(value.toString)
      log.debug(s"[Iteration $iteration|fixed: ${fixed.size} Mapper value: ${value.toString}, fixed: ${fixed.toString()}")
      if (fixed.size > 0) {
        val execStats = searchForLiterals(fixed, List(), context, depth);

        log.info(
          s"""
             |##################    Mapper Stats   ###################
             |Mapper thread ID: ${Thread.currentThread().getId}
              ||Mapper thread Name: ${Thread.currentThread().getName}
              |Iteration $iteration
              |fixed: ${fixed.size}
              |ExecTime: ${(System.currentTimeMillis() - start) / 1000} seconds
                                                                         |Sols found : ${execStats._1}|Pruned: ${execStats._2}
              |########################################################
       """.stripMargin);
      }
    }

  }


}
