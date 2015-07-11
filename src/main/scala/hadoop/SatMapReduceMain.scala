package hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ToolRunner
import utils.HBaseHelper


/**
 * Created by marbarfa on 1/13/14.
 * Main MapReduce program. This program is the main job for the MapReduce SAT solver.
 */
object SatMapReduceMain extends HBaseHelper {

  def main(args: Array[String]) {

    var execSum : Long = 0
    var arguments = SatMapReduceJob.parseArgs(args)

    if (arguments!= null){

      log.info(
        s"""
           |Problem instance: ${arguments._1}
           |depth: ${arguments._2}
           |number of mappers: ${arguments._3}
           |iterations: ${arguments._4}
         """.stripMargin)

      initHTable()

      var solutions  = List[String]()
      for(i <- 0 until arguments._4){
        var res = ToolRunner.run(new Configuration(), SatMapReduceJob, args);
        if (res == 0){
          log.info(s"Retrieving soluton with value ${arguments._1}")
          val sol = retrieveSolution(arguments._1)
          solutions ++= List(sol._1)
          execSum += sol._2
          log.info(s"Iteration ${arguments._4} finished with millis ${sol._2}")
        } else {
          System.exit(res)
        }
      }

      log.info(
        s"""
           |Execution time (total): $execSum
           |Problem: ${arguments._1}
           |Number of iterations: ${arguments._4}
           |Execution time (AVG x iteration): ${execSum / arguments._4}
         """.stripMargin)

      var solutionsStr = solutions.foldLeft(""){(accum, b) => if (accum == "") b else accum + ";" + b}

      log.info(
        s"""
           |SOLUTIONS
           |$solutionsStr
         """.stripMargin)

      if (solutions.size > 0)
        saveToHBaseSolFoundSummary(solutionsStr, arguments._1, (execSum / arguments._4).toString)

      cleanup(arguments._1)
    }

    System.exit(if(execSum!=0) 0 else 1);
  }



}
