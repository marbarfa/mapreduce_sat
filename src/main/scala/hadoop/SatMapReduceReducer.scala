package main.scala.hadoop

import java.lang
import main.java.enums.EnumMRCounters
import main.scala.common.{SatMapReduceConstants, SatMapReduceHelper}
import main.scala.domain.Formula
import main.scala.utils.{HBaseHelper, SatLoggingUtils, ConvertionHelper, CacheHelper}
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
  var withHbase = false
  var startupTime : Long = _
  type Context = Reducer[LongWritable, Text, NullWritable, Text]#Context

  override def setup(context: Context) {
    // retrieve 3SAT instance.
    satProblem = context.getConfiguration.get("problem_path");
    if (formula == null) {
      formula = CacheHelper.sat_instance(satProblem)
    }

    iteration = context.getConfiguration.getInt("iteration", 1);
    startTime = context.getConfiguration.getLong("start_miliseconds", 0);
    fixedLiteralsNumber = context.getConfiguration.getInt("fixed_literals", 0);

    if (withHbase)
      initHTable()
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
        | ##########################################################################
        | Solution found in ${(System.currentTimeMillis() - startTime) / 1000} seconds
        | Solution found in interation $iteration.
        | SAT Problem: $satProblem
        | --------------------------------------------------------------------------
        | Solution: $solutionString
        | ##########################################################################
      """.stripMargin,
      s"${SatMapReduceConstants.sat_solution_path}-${satProblem.split("/").last}-${startTime.toString}", true);
  }

  override def reduce(key: LongWritable, values: lang.Iterable[Text], context: Context) {
    values.asScala.foreach(v => {
      val literalDefinition = SatMapReduceHelper.parseInstanceDef(v.toString.trim)
      if (formula.n == literalDefinition.size) {
        log.debug(s"All literals are set, possible solution found!: ${v.toString.trim}")
        //all literals are set.
        if (formula.isSatisfasiable(literalDefinition, log)) {
          log.info(s"Solution found = ${literalDefinition.toString()}!!!")
          saveSolution(literalDefinition);
          context.getCounter(EnumMRCounters.SOLUTIONS).increment(1);
        } else {
          log.info(s"Solution ${literalDefinition.toString()} not satisfasiable!")
        }
      } else {
        context.getCounter(EnumMRCounters.SUBPROBLEMS).increment(1);
        //still a partial solution
        var ps = SatMapReduceHelper.createSatString(literalDefinition)
        log.info(s"Partial solution: ${ps} of $literalDefinition")
        context.write(NullWritable.get(), new Text(ps.getBytes))
      }
    })
  }


}
