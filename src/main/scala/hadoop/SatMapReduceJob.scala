package hadoop

import java.util.Date
import enums.EnumMRCounters
import common.{SatMapReduceConstants, SatMapReduceHelper}
import hadoop.SatMapReduceMain._
import utils.HBaseHelper
import utils._
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Delete, Scan, ResultScanner, HTable, Result}
import org.apache.hadoop.io.{NullWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, NLineInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool
import scala.collection.JavaConverters._


/**
 * Created by marbarfa on 1/13/14.
 * Main MapReduce program. This program is the main job for the MapReduce SAT solver.
 */
object SatMapReduceJob extends Configured with Tool with SatLoggingUtils with HBaseHelper {

  var instance_path: String = _
  var startTime: Long = _
  var depth: Int = _
  var numberOfMappers: Int = _
  var numberOfLiterals: Int = _
  val withHbase = true;

  /**
   * Main SAT program.
   * @param args
   * @return
   */
  def run(args: Array[String]): Int = {

    //retrieve problem partition file
    var arguments = parseArgs(args)

    if (arguments == null){
      log.info(
        s"""
           |Wrong number of inputs. The app needs 2 parameters:
           |input_path number_of_splits with:
           |input_path : where input files are located.
           |depth: how many literals to fix in each iteration.
           |number of mappers: max mappers to create in each iteration.
         """.stripMargin
      )
    } else {

      startTime = System.currentTimeMillis();
      instance_path = arguments._1
      depth = arguments._2
      numberOfMappers = arguments._3

      log.info("Starting mapreduce algorithm...")
      log.info("Cleaning previous information...")
      initHTable();

      cleanup(instance_path)


      var job: SatJob = createInitJob();

      var finishedOk: Boolean = job.waitForCompletion(true)
      var end = false;
      while (finishedOk && !end) {
        log.info(s"#################################################################################################")
        log.info(s"Retrieving soluton with value ${instance_path}")
        var solFound = retrieveSolution(instance_path)
        if (solFound != null) {
          log.info(s"Solution found!, finishing algorithm....")
          finishedOk= true;
          end = true;
        } else {
          log.info(s"#################################################################################################")
          log.info(s"####################Solution not found, starting iteration ${job.iteration + 1}################")
          log.info(s"#################################################################################################")
          //solution not found yet => start next iteration.(input = previous output, output = new tmp
          var fixedLit = job.getConfiguration.getInt("fixed_literals", 0);
          log.info(s"##### Fixed literals configuration: $fixedLit")

          if (fixedLit < numberOfLiterals) {

            //Save current status to file.
            SatMapReduceHelper.saveStringToFile(
              s"""
                 |Time: ${(System.currentTimeMillis() - startTime) / 1000} seconds
                                                                            |Interation: ${job.iteration}|Fixed: $fixedLit
              """.stripMargin,
              s"${SatMapReduceConstants.sat_exec_evolution}-${instance_path.split("/").last}-${startTime}", true);

            var subprobCounter = job.getCounters.findCounter(EnumMRCounters.SUBPROBLEMS)
            var subproblemsCount = subprobCounter.getValue.toInt

            job = createNewJob(job.output,
              SatMapReduceConstants.sat_tmp_folder_output + "_" + (job.iteration + 1),
              job.iteration + 1,
              fixedLit + depth,
              subproblemsCount)

            finishedOk = job.waitForCompletion(true);
            var tasks = job.getConfiguration.get("mapreduce.job.maps");
            log.info(s"Job tasks $tasks")
            if (tasks != null && tasks.equals("0")) {
              log.info(s"Finishing MR job without solutions!.")

              SatMapReduceHelper.saveStringToFile(
                s"""
                   |##########################################################################
                   |Solution not found
                   |Time: ${(System.currentTimeMillis() - startTime) / 1000} seconds
                                                                              |Interation: ${job.iteration}.
                                                                                                             |Problem: $instance_path
                    |##########################################################################
              """.stripMargin,
                s"${SatMapReduceConstants.sat_not_solution_path}-${instance_path.split("/").last}-${startTime.toString}", true);

              end = true;
            }
          } else {
            end = true;
          }
        }
      }

      if (finishedOk) {
        return 0;
      } else {
        return 1;
      }
    }

    return 1

  }


  private def createInitJob(): SatJob = {
    var time = new Date()
    var input_path = SatMapReduceConstants.sat_tmp_folder_input + s"_${time.getTime}"

    var formula = SatReader.read3SatInstance(instance_path);
    //the first time, upload the default formula to HBASE
    saveToHBaseFormula(SatMapReduceConstants.HBASE_FORMULA_DEFAULT, formula, instance_path)

    numberOfLiterals = formula.n;

    var initialDepth = Math.sqrt(numberOfMappers * 2).toInt;
    //generate problem split -> first choose which literals use as variables and how many.
    var problemSplitVars = SatMapReduceHelper.generateProblemSplit(List(), initialDepth, formula);
    SatMapReduceHelper.genearteProblemMap(problemSplitVars, new ISatCallback[List[Int]] {
      override def apply(t: List[Int]) =
      //save problem definition in the input path to be used as input in the MapReduce algorithm.
        SatMapReduceHelper.saveProblemSplit(t, input_path);
    })

    var job = createNewJob(input_path,
      SatMapReduceConstants.sat_tmp_folder_output + "_1",
      1,
      initialDepth,
      math.pow(2, initialDepth).toInt);

    return job;
  }


  private def createNewJob(input: String,
                           output: String,
                           iteration: Int,
                           fixedLiterals: Int,
                           numberOfProblems: Int): SatJob = {
    val job = new SatJob(input, output, iteration, getConf, SatMapReduceJob.getClass.getSimpleName)

    job.setJarByClass(SatMapReduceJob.getClass)
    job.setMapperClass(classOf[SatMapReduceMapper])
    job.setReducerClass(classOf[SatMapReduceReducer])

    job.setOutputKeyClass(classOf[LongWritable])
    job.setOutputValueClass(classOf[Text])

    var lines_per_map = numberOfProblems / numberOfMappers;
    if (lines_per_map < 1) {
      lines_per_map = 1;
    }

    log.info(
      s"""
         |Creating job with parameters:
         |iteration            : ${iteration.toString}
          |depth                : ${depth.toString}
          |path                 : $instance_path
          |fixed literals       : $fixedLiterals
          |mappers              : $numberOfMappers
          |number Of problems   : $numberOfProblems
          |lines/map            : $lines_per_map
       """.stripMargin)

    job.getConfiguration.set(SatMapReduceConstants.config.iteration, iteration.toString);
    job.getConfiguration.set(SatMapReduceConstants.config.depth, depth.toString);
    job.getConfiguration.set(SatMapReduceConstants.config.start_miliseconds, startTime.toString);
    job.getConfiguration.set(SatMapReduceConstants.config.problem_path, instance_path);
    job.getConfiguration.setInt(SatMapReduceConstants.config.fixed_literals, fixedLiterals);
    job.getConfiguration.setInt("mapreduce.input.lineinputformat.linespermap", lines_per_map);

    job.setNumReduceTasks(lines_per_map);
    //    job.getCounters.addGroup(classOf[EnumMRCounters].getName, EnumMRCounters.SUBPROBLEMS.toString)
    //    job.getCounters.addGroup(classOf[EnumMRCounters].getName, EnumMRCounters.SOLUTIONS.toString)


    //use NLineInputFormat => each mapper will receive one line of the file

    FileInputFormat.setInputPaths(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output))

    job.setInputFormatClass(classOf[NLineInputFormat]);

    //Upload sat problem to the Zookeeper.
    CacheHelper.putSatInstance(job, instance_path)
    return job
  }


  /**
   * Parses the input parameters
   * @param args
   * @return (input_path, depth, number_of_mappers, iterations)
   */
  def parseArgs(args: Array[String]): (String, Int, Int, Int) ={

    var instance_path="";
    var depth=1
    var numberOfMappers= 10
    var problem_iterations = 1

    //retrieve problem partition file
    for(a <- args){
      var param = a.split("=");
      if (param.size > 1){
        param.apply(0) match {
          case "input_path" => {instance_path = param.apply(1)}
          case "depth" => {depth = param.apply(1).toInt}
          case "mappers" => {numberOfMappers = param.apply(1).toInt}
          case "iterations" => {problem_iterations = param.apply(1).toInt}
        }
      }
    }

    if (instance_path.isEmpty ||
      depth < 0 ||
      numberOfMappers < 0){
      log.info(
        s"""
           |Wrong number of inputs.
           |Valid parameters:
           |input_path=file_path
           |depth=depth_to_use_in_the_DFS_algorithm
           |mappers=number_of_mappers_to_use
           |iterations=number_of_times_to_run
         """.stripMargin
      )
      return null;
    }

    return (instance_path, depth, numberOfMappers, problem_iterations)
  }




}
