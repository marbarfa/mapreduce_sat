package main.scala.hadoop

import java.util.Date
import main.scala.common.{SatMapReduceConstants, SatMapReduceHelper}
import main.scala.utils.{CacheHelper, ISatCallback, SatReader}
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
object SatMapReduceJob extends Configured with Tool {


  /**
   * Main SAT program.
   * @param args
   * @return
   */
  def run(args: Array[String]): Int = {

    //retrieve problem partition file
    if (args.size != 2) {
      println("Wrong number of inputs. The app needs 2 parameters: \n" +
        "input_path output_path with:\n" +
        "input_path : where input files are located.\n" +
        "output_path: where output files will be saved\n")
    } else {
      println ("Starting mapreduce algorithm...")
      var job : SatJob = createInitJob(args(0));
      var finishedOk: Boolean = job.waitForCompletion(true)
      var end = false;
      while (finishedOk && !end){
        if (SatReader.readSolution()) {
          //job ended with solution found!
          //save solution to output.
          val fs = FileSystem.get(new Configuration())
          fs.rename(new Path(job.output), new Path(args(1)));
          end = true;
        } else {
          println (s"Solution not found, starting iteration ${job.iteration + 1}")
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
    var job = createNewJob(SatMapReduceConstants.sat_tmp_folder_input, SatMapReduceConstants.sat_tmp_folder_output + "_1", 1);

    var formula = SatReader.read3SatInstance(input);

    //generate problem split -> first choose which literals use as variables and how many.
    var problemSplitVars = SatMapReduceHelper.generateProblemSplit(List(), formula.n, SatMapReduceConstants.variable_literals_amount);
    var time = new Date()
    SatMapReduceHelper.genearteProblemMap(problemSplitVars, new ISatCallback[Map[Int, Boolean]] {
      override def apply(t: Map[Int, Boolean]) =
      //save problem definition in the input path to be used as input in the MapReduce algorithm.
        SatMapReduceHelper.saveProblemSplit(t, SatMapReduceConstants.sat_tmp_folder_input + s"_${time.getTime}");
    })

    //Upload sat problem to the Zookeeper.
    CacheHelper.putSatInstance(job, input)

    return job;
  }



  private def createNewJob(input: String, output: String, iteration : Int): SatJob = {
    val job = new SatJob(input, output, iteration, getConf, SatMapReduceJob.getClass.getSimpleName)
    job.setJarByClass(SatMapReduceJob.getClass)
    job.setMapperClass(classOf[SatMapReduceMapper])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    //use NLineInputFormat => each mapper will receive one line of the file
    job.setInputFormatClass(classOf[NLineInputFormat]);

    FileInputFormat.setInputPaths(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output))

    return job
  }

}
