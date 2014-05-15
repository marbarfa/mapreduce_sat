package main.scala.hadoop

import java.lang
import main.scala.common.{SatMapReduceConstants, SatMapReduceHelper}
import main.scala.domain.Formula
import main.scala.utils.{SatLoggingUtils, ConvertionHelper, CacheHelper}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConverters._

/**
  * Created by marbarfa on 1/13/14.
  */
class SatMapReduceReducer extends Reducer[Text,Text,NullWritable,Text] with ConvertionHelper with SatLoggingUtils{

  var formula : Formula = _
  var table : HTable = _
  var startTime : Long = _
  var iteration : Int = _
  var satProblem: String = _

  override def setup(context: Reducer[Text,Text,NullWritable,Text]#Context) {
    // retrieve 3SAT instance.
    satProblem = context.getConfiguration.get("problem_path");
    if (formula == null) {
      formula = CacheHelper.sat_instance(satProblem)
    }

    iteration = context.getConfiguration.getInt("iteration", 1);
    startTime = context.getConfiguration.getLong("start_miliseconds", 0);

    // Get HBase table of invalid variable combination
    val hconf = HBaseConfiguration.create
    table = new HTable(hconf, "var_tables")
  }

  protected override def cleanup(context: Reducer[Text,Text,NullWritable,Text]#Context){
    formula = null;
    table = null;
  }

  def saveSolution(solutionMap: Set[Int]) = {
    log.debug(s"Saving solution ${solutionMap.toString()} to ${SatMapReduceConstants.sat_solution_path}")
    //save solution to file.
    var solutionString = SatMapReduceHelper.createSatString(solutionMap);

    SatMapReduceHelper.saveStringToFile(
      s"""
        | ##########################################################################
        | Solution found in ${(System.currentTimeMillis() - startTime)/1000} seconds
        | Solution found in interation $iteration.
        | SAT Problem: $satProblem
        | --------------------------------------------------------------------------
        | Solution: $solutionString
        | ##########################################################################
      """.stripMargin,
      SatMapReduceConstants.sat_solution_path + satProblem, true);
  }

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text,Text,NullWritable,Text]#Context) {
       values.asScala.foreach(v => {
         var literalDefinition = SatMapReduceHelper.parseInstanceDef(key.toString.trim + " " + v.toString.trim)
          if (formula.n == literalDefinition.size) {
            log.debug(s"All literals are set, possible solution found!: ${key.toString.trim + " " + v.toString.trim}")
            //all literals are set.
            if (formula.isSatisfasiable(literalDefinition)) {
              log.info(s"Solution found = ${literalDefinition.toString()}!!!")
              saveSolution(literalDefinition);
              context.getConfiguration.set("sol_found", "true");
            } else {
              log.info(s"Solution ${literalDefinition.toString()} not satisfasiable!")
            }
          }else{
            //still a partial solution
            context.write(NullWritable.get(), new Text(SatMapReduceHelper.createSatString(literalDefinition).getBytes))
          }
       })
  }


 }
