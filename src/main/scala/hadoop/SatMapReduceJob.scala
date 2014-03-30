package main.scala.hadoop

import java.util.Date
import main.scala.common.{SatMapReduceConstants, SatMapReduceHelper}
import main.scala.utils.{SatLoggingUtils, CacheHelper, ISatCallback, SatReader}
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, NLineInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool


/**
 * Created by marbarfa on 1/13/14.
 * Main MapReduce program. This program is the main job for the MapReduce SAT solver.
 */
object SatMapReduceJob extends Configured with Tool with SatLoggingUtils {

  var instance_path : String = _

  /**
   * Main SAT program.
   * @param args
   * @return
   */
  def run(args: Array[String]): Int = {

    //retrieve problem partition file
    if (args.size != 2) {
      log.info(s"Wrong number of inputs. The app needs 2 parameters: \n" +
        "input_path output_path with:\n" +
        "input_path : where input files are located.\n" +
        "output_path: where output files will be saved\n")
    } else {
      log.info("Starting mapreduce algorithm...")
      instance_path = args(0)
      var job : SatJob = createInitJob(instance_path);
      var finishedOk: Boolean = job.waitForCompletion(true)
      var end = false;
      while (finishedOk && !end){
        if (SatReader.readSolution()) {
          log.info(s"Solution file found!, finishing algorithm....")
          //job ended with solution found!
          //save solution to output.
          log.info(s"Rename/Move from ${SatMapReduceConstants.sat_solution_path} to ${args(1)}")
          val fs = FileSystem.get(new Configuration())
          fs.rename(new Path(SatMapReduceConstants.sat_solution_path), new Path(args(1)));
          end = true;
        } else {
          log.info (s"Solution not found, starting iteration ${job.iteration + 1}")
          //solution not found yet => start next iteration.(input = previous output, output = new tmp
          job = createNewJob(job.output, SatMapReduceConstants.sat_tmp_folder_output + (job.iteration + 1), job.iteration + 1)
          finishedOk = job.waitForCompletion(true);
        }
      }

      if (finishedOk){
        return 0;
      }else{
        return 1;
      }
    }

    return 1

  }

  private def createInitJob(input: String): SatJob = {
    var time = new Date()
    var input_path = SatMapReduceConstants.sat_tmp_folder_input + s"${time.getTime}"
    var job = createNewJob(input_path, SatMapReduceConstants.sat_tmp_folder_output + "_1", 1);

    var formula = SatReader.read3SatInstance(input);

    //generate problem split -> first choose which literals use as variables and how many.
    var problemSplitVars = SatMapReduceHelper.generateProblemSplit(List(), formula.n, SatMapReduceConstants.variable_literals_amount);
    SatMapReduceHelper.genearteProblemMap(problemSplitVars, new ISatCallback[Set[Int]] {
      override def apply(t: Set[Int]) =
      //save problem definition in the input path to be used as input in the MapReduce algorithm.
        SatMapReduceHelper.saveProblemSplit(t, input_path);
    })

    return job;
  }



  private def createNewJob(input: String, output: String, iteration : Int): SatJob = {
    val job = new SatJob(input, output, iteration, getConf, SatMapReduceJob.getClass.getSimpleName)

    job.setJarByClass(SatMapReduceJob.getClass)
    job.setMapperClass(classOf[SatMapReduceMapper])
    job.setReducerClass(classOf[SatMapReduceReducer])


    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    //use NLineInputFormat => each mapper will receive one line of the file

    FileInputFormat.setInputPaths(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output))

    job.setInputFormatClass(classOf[NLineInputFormat]);

    //Upload sat problem to the Zookeeper.
    CacheHelper.putSatInstance(job, instance_path)
    return job
  }

}
